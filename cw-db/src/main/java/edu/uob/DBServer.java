package edu.uob;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.util.*;

/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;

    // 存放数据库文件夹的路径（如 "...\databases"）
    private final String storageFolderPath;

    // 记录当前选用的数据库名（用户通过 USE 命令进行切换）
    private String currentDatabaseName;

    // 缓存已加载的数据库对象（数据库名小写 -> Database 对象）
    private final Map<String, Database> databases = new HashMap<>();

    public static void main(String[] args) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
     * KEEP this signature otherwise we won't be able to mark your submission correctly.
     */
    public DBServer() {
        // 设置数据库所在文件夹
        this.storageFolderPath = Paths.get("databases").toAbsolutePath().toString();

        // 若文件夹不存在则创建
        try {
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch (IOException ioe) {
            System.err.println("Can't create database storage folder " + storageFolderPath);
        }

        // 你可以选择在此预先扫描并加载所有数据库，也可以在首次使用时惰性加载
        this.currentDatabaseName = null;
    }

    /**
     * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise
     * we won't be able to mark your submission correctly.
     *
     * <p>This method handles all incoming DB commands and carries out the required actions.
     */
    public String handleCommand(String command) {
        if (command == null || command.trim().isEmpty()) {
            return "[ERROR] Empty command.";
        }

        try {
            // 预处理：去除前后空格，检查是否以 ";" 结尾
            String cleaned = command.trim();
            if (!cleaned.endsWith(";")) {
                return "[ERROR] Missing semicolon at the end of command";
            }
            // 去掉尾部的 ";"
            cleaned = cleaned.substring(0, cleaned.length() - 1).trim();

            // 分发解析执行
            return parseAndExecute(cleaned);

        } catch (DBException e) {
            // 命令处理过程中的自定义异常（语法错误、不存在的库表等）
            return "[ERROR] " + e.getMessage();
        } catch (Exception e) {
            // 其他未知异常，保证服务器不会崩溃
            return "[ERROR] Unhandled exception: " + e.getMessage();
        }
    }

    // ========================= Networking (无需修改) ==============================

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
             BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }

    // ============================ 命令解析和执行 ============================

    private String parseAndExecute(String cmd) throws DBException, IOException {
        // 简化写法：将关键字大写，用 startsWith() 做判断
        // 若需解析 WHERE、JOIN 条件，则要更精细的拆分
        String upper = cmd.toUpperCase(Locale.ROOT).trim();

        if (upper.startsWith("USE ")) {
            return handleUse(cmd);
        } else if (upper.startsWith("CREATE DATABASE")) {
            return handleCreateDatabase(cmd);
        } else if (upper.startsWith("CREATE TABLE")) {
            return handleCreateTable(cmd);
        } else if (upper.startsWith("DROP DATABASE")) {
            return handleDropDatabase(cmd);
        } else if (upper.startsWith("DROP TABLE")) {
            return handleDropTable(cmd);
        } else if (upper.startsWith("ALTER TABLE")) {
            return handleAlterTable(cmd);
        } else if (upper.startsWith("INSERT INTO")) {
            return handleInsert(cmd);
        } else if (upper.startsWith("SELECT")) {
            return handleSelect(cmd);
        } else if (upper.startsWith("UPDATE")) {
            return handleUpdate(cmd);
        } else if (upper.startsWith("DELETE FROM")) {
            return handleDelete(cmd);
        } else if (upper.startsWith("JOIN ")) {
            return handleJoin(cmd);
        } else {
            throw new DBException("Unrecognised or unsupported command");
        }
    }

    /**
     * USE dbName
     */
    private String handleUse(String cmd) throws DBException, IOException {
        // 形如 "USE myDatabase"
        String[] parts = cmd.split("\\s+");
        if (parts.length < 2) {
            throw new DBException("Database name missing in USE command");
        }
        String dbName = parts[1].toLowerCase(Locale.ROOT);
        Path dbPath = Paths.get(storageFolderPath, dbName);
        if (!Files.exists(dbPath)) {
            throw new DBException("Database does not exist: " + dbName);
        }
        // 切换当前数据库
        currentDatabaseName = dbName;
        // 如有需要，惰性加载
        loadDatabaseIfNeeded(dbName);
        return "[OK]";
    }

    /**
     * CREATE DATABASE dbName
     */
    private String handleCreateDatabase(String cmd) throws DBException, IOException {
        // 形如 "CREATE DATABASE myDB"
        String[] parts = cmd.split("\\s+");
        if (parts.length < 3) {
            throw new DBException("Database name missing in CREATE DATABASE");
        }
        String dbName = parts[2].toLowerCase(Locale.ROOT);
        Path dbPath = Paths.get(storageFolderPath, dbName);
        if (Files.exists(dbPath)) {
            throw new DBException("Database already exists: " + dbName);
        }
        Files.createDirectory(dbPath);
        return "[OK]";
    }

    /**
     * CREATE TABLE tableName (col1, col2, ...)
     */
    private String handleCreateTable(String cmd) throws DBException, IOException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected. Use 'USE dbName' first.");
        }

        String upper = cmd.toUpperCase();
        int idxTable = upper.indexOf("TABLE") + 5; // "TABLE" 长度
        int idxParen = cmd.indexOf("(");
        if (idxTable < 0 || idxParen < 0 || idxParen <= idxTable) {
            throw new DBException("Syntax error in CREATE TABLE");
        }
        String tableName = cmd.substring(idxTable, idxParen).trim().toLowerCase(Locale.ROOT);

        int idxClose = cmd.lastIndexOf(")");
        if (idxClose < idxParen) {
            throw new DBException("Missing ) in CREATE TABLE");
        }
        String colsStr = cmd.substring(idxParen + 1, idxClose).trim();
        if (colsStr.isEmpty()) {
            throw new DBException("No columns specified in CREATE TABLE");
        }
        String[] rawCols = colsStr.split(",");
        List<String> colList = new ArrayList<>();
        for (String c : rawCols) {
            String colName = c.trim();
            if (!colName.isEmpty()) {
                colList.add(colName);
            }
        }

        loadDatabaseIfNeeded(currentDatabaseName);
        Database db = databases.get(currentDatabaseName);
        db.createTable(tableName, colList);

        return "[OK]";
    }

    /**
     * DROP DATABASE dbName
     */
    private String handleDropDatabase(String cmd) throws DBException, IOException {
        String[] parts = cmd.split("\\s+");
        if (parts.length < 3) {
            throw new DBException("Database name missing in DROP DATABASE");
        }
        String dbName = parts[2].toLowerCase(Locale.ROOT);
        Path dbPath = Paths.get(storageFolderPath, dbName);
        if (!Files.exists(dbPath)) {
            throw new DBException("Database does not exist: " + dbName);
        }
        deleteDirectoryRecursively(dbPath.toFile());
        // 移除内存中的缓存
        databases.remove(dbName);
        if (dbName.equals(currentDatabaseName)) {
            currentDatabaseName = null;
        }
        return "[OK]";
    }

    /**
     * DROP TABLE tableName
     */
    private String handleDropTable(String cmd) throws DBException, IOException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected.");
        }
        String[] parts = cmd.split("\\s+");
        if (parts.length < 3) {
            throw new DBException("Table name missing in DROP TABLE");
        }
        String tableName = parts[2].toLowerCase(Locale.ROOT);

        loadDatabaseIfNeeded(currentDatabaseName);
        Database db = databases.get(currentDatabaseName);
        db.dropTable(tableName);

        return "[OK]";
    }

    /**
     * ALTER TABLE tableName ADD COLUMN colName  或  ALTER TABLE tableName DROP COLUMN colName
     */
    private String handleAlterTable(String cmd) throws DBException, IOException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected.");
        }
        String upper = cmd.toUpperCase(Locale.ROOT);
        int idxTable = upper.indexOf("TABLE") + 5;
        String rest = cmd.substring(idxTable).trim();
        String[] parts = rest.split("\\s+");
        if (parts.length < 4) {
            throw new DBException("Syntax error in ALTER TABLE");
        }
        String tableName = parts[0].toLowerCase(Locale.ROOT);
        String op1 = parts[1].toUpperCase(Locale.ROOT);
        String op2 = parts[2].toUpperCase(Locale.ROOT);
        String colName = parts[3];

        // 仅支持 ADD COLUMN / DROP COLUMN
        String operation = op1 + " " + op2;
        loadDatabaseIfNeeded(currentDatabaseName);
        Database db = databases.get(currentDatabaseName);
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DBException("No such table: " + tableName);
        }

        if ("ADD COLUMN".equals(operation)) {
            table.addColumn(colName);
        } else if ("DROP COLUMN".equals(operation)) {
            table.dropColumn(colName);
        } else {
            throw new DBException("Unsupported ALTER operation: " + operation);
        }
        table.saveToDisk();
        return "[OK]";
    }

    /**
     * INSERT INTO tableName VALUES (val1, val2, ...)
     */
    private String handleInsert(String cmd) throws DBException, IOException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected.");
        }
        String upper = cmd.toUpperCase(Locale.ROOT);
        int idxInto = upper.indexOf("INTO");
        int idxValues = upper.indexOf("VALUES");
        if (idxInto < 0 || idxValues < 0) {
            throw new DBException("Syntax error in INSERT INTO");
        }
        // 提取表名
        String tableName = cmd.substring(idxInto + 4, idxValues).replace("VALUES", "").trim().toLowerCase(Locale.ROOT);

        // 提取值
        int idxOpen = cmd.indexOf("(", idxValues);
        int idxClose = cmd.lastIndexOf(")");
        if (idxOpen < 0 || idxClose < 0) {
            throw new DBException("Missing parentheses in INSERT INTO");
        }
        String valuesPart = cmd.substring(idxOpen + 1, idxClose).trim();
        String[] rawVals = valuesPart.split(",");
        List<String> valList = new ArrayList<>();
        for (String rv : rawVals) {
            // 去掉可能的引号
            rv = rv.trim().replaceAll("^'(.*)'$", "$1");
            valList.add(rv);
        }

        loadDatabaseIfNeeded(currentDatabaseName);
        Database db = databases.get(currentDatabaseName);
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DBException("No such table: " + tableName);
        }
        table.insertRow(valList);
        table.saveToDisk();

        return "[OK]";
    }

    /**
     * SELECT col1, col2 FROM tableName WHERE ...
     */
    private String handleSelect(String cmd) throws DBException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected.");
        }
        String upper = cmd.toUpperCase(Locale.ROOT);
        int idxSelect = upper.indexOf("SELECT") + 6;
        int idxFrom = upper.indexOf("FROM");
        if (idxFrom < 0) {
            throw new DBException("Syntax error in SELECT, missing FROM");
        }
        String selectCols = cmd.substring(idxSelect, idxFrom).trim();
        List<String> colList;
        if (selectCols.equals("*")) {
            colList = null; // 标识查询所有列
        } else {
            // 以逗号分隔
            colList = new ArrayList<>();
            for (String c : selectCols.split(",")) {
                colList.add(c.trim());
            }
        }

        int idxWhere = upper.indexOf("WHERE");
        String tableName;
        String whereClause = null;
        if (idxWhere < 0) {
            tableName = cmd.substring(idxFrom + 4).trim().toLowerCase(Locale.ROOT);
        } else {
            tableName = cmd.substring(idxFrom + 4, idxWhere).trim().toLowerCase(Locale.ROOT);
            whereClause = cmd.substring(idxWhere + 5).trim();
        }

        Database db = databases.get(currentDatabaseName);
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DBException("No such table: " + tableName);
        }

        // 获取匹配行
        List<Row> matched = table.select(whereClause);

        // 确定要输出的列
        List<String> finalCols;
        if (colList == null) {
            finalCols = table.getColumnNames(); // 全部
        } else {
            finalCols = colList;
        }

        // 组装输出
        StringBuilder sb = new StringBuilder();
        sb.append("[OK]\n");
        // 打印列名行
        for (int i = 0; i < finalCols.size(); i++) {
            sb.append(finalCols.get(i));
            if (i < finalCols.size() - 1) {
                sb.append("\t");
            }
        }
        sb.append("\n");
        // 打印匹配行
        for (Row r : matched) {
            for (int i = 0; i < finalCols.size(); i++) {
                String col = finalCols.get(i);
                sb.append(r.getValue(table, col));
                if (i < finalCols.size() - 1) {
                    sb.append("\t");
                }
            }
            sb.append("\n");
        }
        return sb.toString().trim();
    }

    /**
     * UPDATE tableName SET col1=val1,... WHERE ...
     */
    private String handleUpdate(String cmd) throws DBException, IOException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected.");
        }
        String upper = cmd.toUpperCase(Locale.ROOT);
        int idxUpdate = upper.indexOf("UPDATE") + 6;
        int idxSet = upper.indexOf("SET");
        if (idxSet < 0) {
            throw new DBException("Missing SET in UPDATE");
        }
        String tableName = cmd.substring(idxUpdate, idxSet).trim().toLowerCase(Locale.ROOT);

        int idxWhere = upper.indexOf("WHERE");
        String setPart;
        String wherePart = null;
        if (idxWhere < 0) {
            setPart = cmd.substring(idxSet + 3).trim();
        } else {
            setPart = cmd.substring(idxSet + 3, idxWhere).trim();
            wherePart = cmd.substring(idxWhere + 5).trim();
        }

        // 解析 SET col=val 列表
        String[] assignments = setPart.split(",");
        Map<String, String> colToVal = new HashMap<>();
        for (String a : assignments) {
            String[] cv = a.split("=");
            if (cv.length != 2) {
                throw new DBException("Syntax error in UPDATE SET");
            }
            String cName = cv[0].trim();
            String val = cv[1].trim().replaceAll("^'(.*)'$", "$1");
            colToVal.put(cName, val);
        }

        loadDatabaseIfNeeded(currentDatabaseName);
        Database db = databases.get(currentDatabaseName);
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DBException("No such table: " + tableName);
        }
        table.updateRows(wherePart, colToVal);
        table.saveToDisk();
        return "[OK]";
    }

    /**
     * DELETE FROM tableName WHERE ...
     */
    private String handleDelete(String cmd) throws DBException, IOException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected.");
        }
        String upper = cmd.toUpperCase(Locale.ROOT);
        int idxFrom = upper.indexOf("FROM") + 4;
        int idxWhere = upper.indexOf("WHERE");

        String tableName;
        String wherePart = null;
        if (idxWhere < 0) {
            tableName = cmd.substring(idxFrom).trim().toLowerCase(Locale.ROOT);
        } else {
            tableName = cmd.substring(idxFrom, idxWhere).trim().toLowerCase(Locale.ROOT);
            wherePart = cmd.substring(idxWhere + 5).trim();
        }

        loadDatabaseIfNeeded(currentDatabaseName);
        Database db = databases.get(currentDatabaseName);
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DBException("No such table: " + tableName);
        }
        table.deleteRows(wherePart);
        table.saveToDisk();
        return "[OK]";
    }

    /**
     * JOIN tableA AND tableB ON colA AND colB
     */
    private String handleJoin(String cmd) throws DBException {
        if (currentDatabaseName == null) {
            throw new DBException("No database selected.");
        }
        String upper = cmd.toUpperCase(Locale.ROOT);
        int idxJoin = upper.indexOf("JOIN") + 4;
        int idxAnd = upper.indexOf("AND", idxJoin);
        int idxOn = upper.indexOf("ON", idxAnd);

        if (idxAnd < 0 || idxOn < 0) {
            throw new DBException("Syntax error in JOIN. Expect: JOIN tableA AND tableB ON colA AND colB");
        }
        String tableAName = cmd.substring(idxJoin, idxAnd).trim().toLowerCase(Locale.ROOT);
        String secondPart = cmd.substring(idxAnd + 3, idxOn).trim();
        String tableBName = secondPart.toLowerCase(Locale.ROOT);

        String onPart = cmd.substring(idxOn + 2).trim();
        String[] onSplit = onPart.split("\\s+AND\\s+");
        if (onSplit.length != 2) {
            throw new DBException("JOIN syntax error, should be 'colA AND colB'");
        }
        String colA = onSplit[0].trim();
        String colB = onSplit[1].trim();

        // 获取表
        Database db = databases.get(currentDatabaseName);
        Table tableA = db.getTable(tableAName);
        Table tableB = db.getTable(tableBName);
        if (tableA == null || tableB == null) {
            throw new DBException("One of the tables in JOIN does not exist.");
        }

        // 做内连接
        List<Row> joinedRows = new ArrayList<>();
        // 新表的列名(把 id 替换成新的 id)
        List<String> joinedColNames = new ArrayList<>();
        joinedColNames.add("id");

        // A表列(去掉id)，前面加前缀 tableAName.
        for (String col : tableA.getColumnNames()) {
            if (!col.equalsIgnoreCase("id")) {
                joinedColNames.add(tableAName + "." + col);
            }
        }
        // B表列(去掉id)
        for (String col : tableB.getColumnNames()) {
            if (!col.equalsIgnoreCase("id")) {
                joinedColNames.add(tableBName + "." + col);
            }
        }

        // 遍历A表和B表行，满足 colA == colB 则生成新行
        int newId = 1;
        for (Row ra : tableA.rows) {
            String valA = ra.getValue(tableA, colA);
            for (Row rb : tableB.rows) {
                String valB = rb.getValue(tableB, colB);
                if (valA != null && valA.equals(valB)) {
                    // 匹配成功
                    Row newRow = new Row(newId++);
                    // 先把A表除id外的列加入
                    for (String c : tableA.getColumnNames()) {
                        if (!c.equalsIgnoreCase("id")) {
                            String v = ra.getValue(tableA, c);
                            newRow.values.add(v);
                        }
                    }
                    // 再把B表除id外的列加入
                    for (String c : tableB.getColumnNames()) {
                        if (!c.equalsIgnoreCase("id")) {
                            String v = rb.getValue(tableB, c);
                            newRow.values.add(v);
                        }
                    }
                    joinedRows.add(newRow);
                }
            }
        }

        // 结果输出
        StringBuilder sb = new StringBuilder("[OK]\n");
        // 列名行
        sb.append(String.join("\t", joinedColNames)).append("\n");
        // 数据行
        for (Row jr : joinedRows) {
            sb.append(jr.id).append("\t");
            for (int i = 0; i < jr.values.size(); i++) {
                sb.append(jr.values.get(i));
                if (i < jr.values.size() - 1) sb.append("\t");
            }
            sb.append("\n");
        }
        return sb.toString().trim();
    }

    // 递归删除文件夹
    private void deleteDirectoryRecursively(File file) {
        if (file.isDirectory()) {
            for (File f : Objects.requireNonNull(file.listFiles())) {
                deleteDirectoryRecursively(f);
            }
        }
        file.delete();
    }

    private void loadDatabaseIfNeeded(String dbName) throws IOException {
        if (databases.containsKey(dbName)) {
            return; // 已经加载
        }
        Path dbPath = Paths.get(storageFolderPath, dbName);
        if (!Files.exists(dbPath)) {
            throw new IOException("Database folder not found: " + dbName);
        }
        Database db = new Database(storageFolderPath, dbName);
        db.loadAllTables();
        databases.put(dbName, db);
    }
}

// ================== 自定义异常类 ===================
class DBException extends Exception {
    public DBException(String message) {
        super(message);
    }
}

// ================== Database 类 ===================
class Database {
    private final String parentPath;  // "...\databases"
    private final String dbName;      // 数据库名 (小写)
    private final Map<String, Table> tables = new HashMap<>();

    public Database(String parentPath, String dbName) {
        this.parentPath = parentPath;
        this.dbName = dbName;
    }

    // 从文件系统加载所有 .tab 表
    public void loadAllTables() throws IOException {
        Path dbDir = Paths.get(parentPath, dbName);
        File folder = dbDir.toFile();
        File[] tabFiles = folder.listFiles((dir, name) -> name.endsWith(".tab"));
        if (tabFiles == null) {
            return;
        }
        for (File f : tabFiles) {
            String fileName = f.getName();
            // 去掉 ".tab"
            String tName = fileName.substring(0, fileName.length() - 4).toLowerCase(Locale.ROOT);
            Table table = new Table(parentPath, dbName, tName);
            table.loadFromDisk();
            tables.put(tName, table);
        }
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    // 在数据库中新建表
    public void createTable(String tableName, List<String> userCols) throws DBException, IOException {
        if (tables.containsKey(tableName)) {
            throw new DBException("Table already exists: " + tableName);
        }
        Table table = new Table(parentPath, dbName, tableName);
        table.init(userCols);
        table.saveToDisk();
        tables.put(tableName, table);
    }

    // 删除表
    public void dropTable(String tableName) throws DBException, IOException {
        Table t = tables.get(tableName);
        if (t == null) {
            throw new DBException("No such table: " + tableName);
        }
        t.deleteFile();
        tables.remove(tableName);
    }
}

// ================== Table 类 =====================
class Table {
    private final String parentPath; // "...\databases"
    private final String dbName;     // 小写
    private final String tableName;  // 小写

    // 列名（第0列是 "id"）
    private final List<String> columnNames = new ArrayList<>();
    // 行数据
    public final List<Row> rows = new ArrayList<>();
    // 用于记录下一次插入可用的 id
    private int nextId = 1;

    public Table(String parentPath, String dbName, String tableName) {
        this.parentPath = parentPath;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    // 初始化表头 (id + 用户列)
    public void init(List<String> userCols) {
        columnNames.clear();
        columnNames.add("id");
        columnNames.addAll(userCols);
        rows.clear();
        nextId = 1;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void loadFromDisk() throws IOException {
        Path path = getTablePath();
        if (!Files.exists(path)) {
            return; // 表文件不存在，则当作空表
        }
        List<String> lines = Files.readAllLines(path);
        if (lines.isEmpty()) {
            return;
        }
        // 第一行是列名
        columnNames.clear();
        String header = lines.get(0);
        String[] cols = header.split("\t");
        Collections.addAll(columnNames, cols);

        rows.clear();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            // 保留空字符串 => split("\t", -1)
            String[] vals = line.split("\t", -1);
            if (vals.length != columnNames.size()) {
                throw new IOException("Column count mismatch in row " + (i + 1));
            }
            int id;
            try {
                id = Integer.parseInt(vals[0]);
            } catch (NumberFormatException e) {
                throw new IOException("Invalid id: " + vals[0]);
            }
            Row row = new Row(id);
            // 剩余列
            for (int j = 1; j < vals.length; j++) {
                row.values.add(vals[j]);
            }
            rows.add(row);
            if (id >= nextId) {
                nextId = id + 1;
            }
        }
    }

    public void saveToDisk() throws IOException {
        Path path = getTablePath();
        List<String> lines = new ArrayList<>();
        // 写表头
        lines.add(String.join("\t", columnNames));
        // 写每行
        for (Row r : rows) {
            List<String> rowItems = new ArrayList<>();
            // 第0列是 id
            rowItems.add(String.valueOf(r.id));
            // 其余列
            rowItems.addAll(r.values);
            lines.add(String.join("\t", rowItems));
        }
        Files.write(path, lines);
    }

    public void deleteFile() throws IOException {
        Files.deleteIfExists(getTablePath());
    }

    private Path getTablePath() {
        return Paths.get(parentPath, dbName, tableName + ".tab");
    }

    // 新增列
    public void addColumn(String colName) throws DBException {
        if (colName.equalsIgnoreCase("id")) {
            throw new DBException("Cannot add 'id' column");
        }
        // 检查重复
        for (String c : columnNames) {
            if (c.equalsIgnoreCase(colName)) {
                throw new DBException("Column already exists: " + colName);
            }
        }
        columnNames.add(colName);
        // 每行加空字符串
        for (Row r : rows) {
            r.values.add("");
        }
    }

    // 删除列
    public void dropColumn(String colName) throws DBException {
        if (colName.equalsIgnoreCase("id")) {
            throw new DBException("Cannot drop 'id' column");
        }
        // 找到列索引
        int idx = -1;
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(colName)) {
                idx = i;
                break;
            }
        }
        if (idx < 0) {
            throw new DBException("Column not found: " + colName);
        }
        columnNames.remove(idx);
        // idx>0 => 对应 rows[x].values 的 (idx-1)
        for (Row r : rows) {
            int dataIndex = idx - 1;
            if (dataIndex >= 0 && dataIndex < r.values.size()) {
                r.values.remove(dataIndex);
            }
        }
    }

    // 插入
    public void insertRow(List<String> userVals) throws DBException {
        // 用户给出的值数必须等于 (columnNames.size - 1) (因为第0列是id)
        if (userVals.size() != columnNames.size() - 1) {
            throw new DBException("Value count mismatch with table columns");
        }
        Row r = new Row(nextId++);
        r.values.addAll(userVals);
        rows.add(r);
    }

    // 查询
    public List<Row> select(String whereClause) {
        List<Row> matched = new ArrayList<>();
        for (Row r : rows) {
            if (matchWhere(r, whereClause)) {
                matched.add(r);
            }
        }
        return matched;
    }

    // 更新
    public void updateRows(String whereClause, Map<String,String> colToVal) throws DBException {
        for (Row r : rows) {
            if (matchWhere(r, whereClause)) {
                for (Map.Entry<String,String> e : colToVal.entrySet()) {
                    String cName = e.getKey();
                    if (cName.equalsIgnoreCase("id")) {
                        throw new DBException("Cannot update 'id' column");
                    }
                    int idx = findColIndex(cName);
                    if (idx < 0) {
                        throw new DBException("No such column: " + cName);
                    }
                    // row.values 的 0号索引对应 columnNames[1]
                    r.values.set(idx - 1, e.getValue());
                }
            }
        }
    }

    // 删除
    public void deleteRows(String whereClause) {
        rows.removeIf(r -> matchWhere(r, whereClause));
    }

    // 简易匹配 (只处理 col==val 这种情况；需自行扩展)
    private boolean matchWhere(Row row, String whereClause) {
        if (whereClause == null || whereClause.isBlank()) {
            return true;
        }
        String upper = whereClause.toUpperCase(Locale.ROOT);
        if (upper.contains("==")) {
            String[] parts = whereClause.split("==");
            if (parts.length != 2) return false;
            String left = parts[0].trim();
            // 去掉引号
            String right = parts[1].trim().replaceAll("^'(.*)'$", "$1");
            String leftVal = row.getValue(this, left);
            return leftVal != null && leftVal.equals(right);
        }
        // 未实现的情况 => 不匹配
        return false;
    }

    private int findColIndex(String colName) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(colName)) {
                return i;
            }
        }
        return -1;
    }
}

// ================== Row 类 ========================
class Row {
    public int id;
    // 除了id外的列值
    public List<String> values = new ArrayList<>();

    public Row(int id) {
        this.id = id;
    }

    // 获取某列对应的值
    public String getValue(Table table, String colName) {
        List<String> cols = table.getColumnNames();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).equalsIgnoreCase(colName)) {
                if (i == 0) {
                    return String.valueOf(id); // id
                }
                return values.get(i - 1);
            }
        }
        return null;
    }
}
