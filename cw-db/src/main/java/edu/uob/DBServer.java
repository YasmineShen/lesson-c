package edu.uob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** This class implements the DB server. */
public class DBServer {

    private static final char END_OF_TRANSMISSION = 4;
    private String storageFolderPath;
    private String currentDatabaseName;
    // 内存中存储所有数据库（数据库名全部转换为小写）
    private Map<String, Database> databases;

    public static void main(String args[]) throws IOException {
        DBServer server = new DBServer();
        server.blockingListenOn(8888);
    }

    /**
     * KEEP this signature otherwise we won't be able to mark your submission correctly.
     */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        databases = new HashMap<>();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch (IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
        // 加载已有的数据库及表（实现持久化）
        loadDatabases();
    }

    /**
     * 加载存储目录下的所有数据库及其中的表数据
     */
    private void loadDatabases() {
        File storageFolder = new File(storageFolderPath);
        File[] dbDirs = storageFolder.listFiles(File::isDirectory);
        if (dbDirs != null) {
            for (File dbDir : dbDirs) {
                String dbName = dbDir.getName().toLowerCase();
                Database db = new Database(dbName);
                databases.put(dbName, db);
            }
        }
    }

    /**
     * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
     * able to mark your submission correctly.
     *
     * <p>This method handles all incoming DB commands and carries out the required actions.
     */
    public String handleCommand(String command) {
        command = command.trim();
        if (!command.endsWith(";")) {
            return "[ERROR] Command must end with a semicolon";
        }

        // 使用正则表达式匹配各类命令

        // 1. CREATE DATABASE <DatabaseName>;
        Pattern createDatabasePattern = Pattern.compile("(?i)^CREATE\\s+DATABASE\\s+(\\w+)\\s*;$");
        Matcher m = createDatabasePattern.matcher(command);
        if (m.matches()) {
            String dbName = m.group(1).toLowerCase();
            if (databases.containsKey(dbName)) {
                return "[ERROR] Database already exists";
            }
            File dbDir = new File(storageFolderPath, dbName);
            if (!dbDir.exists() && !dbDir.mkdirs()) {
                return "[ERROR] Could not create database directory";
            }
            Database db = new Database(dbName);
            databases.put(dbName, db);
            return "[OK] Database created";
        }

        // 2. USE <DatabaseName>;
        Pattern usePattern = Pattern.compile("(?i)^USE\\s+(\\w+)\\s*;$");
        m = usePattern.matcher(command);
        if (m.matches()) {
            String dbName = m.group(1).toLowerCase();
            if (!databases.containsKey(dbName)) {
                return "[ERROR] Database does not exist";
            }
            currentDatabaseName = dbName;
            return "[OK] Using database " + dbName;
        }

        // 3. CREATE TABLE <TableName> [(<AttributeList>)];
        Pattern createTablePattern = Pattern.compile("(?i)^CREATE\\s+TABLE\\s+(\\w+)(?:\\s*\\(([^)]+)\\))?\\s*;$");
        m = createTablePattern.matcher(command);
        if (m.matches()) {
            String tableName = m.group(1).toLowerCase();
            if (currentDatabaseName == null) {
                return "[ERROR] No database selected";
            }
            Database db = databases.get(currentDatabaseName);
            if (db.tables.containsKey(tableName)) {
                return "[ERROR] Table already exists";
            }
            // 默认总是添加第一列 "id"
            List<String> columns = new ArrayList<>();
            columns.add("id");
            String attrList = m.group(2);
            if (attrList != null) {
                String[] attrs = attrList.split(",");
                for (String attr : attrs) {
                    String trimmed = attr.trim();
                    if (trimmed.equalsIgnoreCase("id")) {
                        return "[ERROR] Cannot use reserved attribute name 'id'";
                    }
                    columns.add(trimmed);
                }
            }
            Table table = new Table(tableName, columns);
            db.tables.put(tableName, table);
            // 在文件系统中创建表文件
            File tableFile = new File(storageFolderPath + File.separator + currentDatabaseName, tableName + ".txt");
            try (BufferedWriter writer = Files.newBufferedWriter(tableFile.toPath())) {
                writer.write(String.join("\t", columns));
                writer.newLine();
            } catch (IOException e) {
                return "[ERROR] Failed to create table file";
            }
            return "[OK] Table created";
        }

        // 4. INSERT INTO <TableName> VALUES (<ValueList>);
        Pattern insertPattern = Pattern.compile("(?i)^INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s*\\((.+)\\)\\s*;$");
        m = insertPattern.matcher(command);
        if (m.matches()) {
            String tableName = m.group(1).toLowerCase();
            if (currentDatabaseName == null) {
                return "[ERROR] No database selected";
            }
            Database db = databases.get(currentDatabaseName);
            if (!db.tables.containsKey(tableName)) {
                return "[ERROR] Table does not exist";
            }
            Table table = db.tables.get(tableName);
            String valuesPart = m.group(2);
            // 简单按逗号分割（假设字符串中不含逗号）
            String[] valueTokens = valuesPart.split(",");
            List<String> values = new ArrayList<>();
            for (String token : valueTokens) {
                String val = token.trim();
                if (val.startsWith("'") && val.endsWith("'") && val.length() >= 2) {
                    val = val.substring(1, val.length() - 1);
                }
                values.add(val);
            }
            if (values.size() != table.columns.size() - 1) {
                return "[ERROR] Incorrect number of values";
            }
            String idStr = String.valueOf(table.nextId);
            table.nextId++;
            List<String> row = new ArrayList<>();
            row.add(idStr);
            row.addAll(values);
            table.rows.add(row);
            // 将新行追加到表文件中
            File tableFile = new File(storageFolderPath + File.separator + currentDatabaseName, tableName + ".txt");
            try (BufferedWriter writer = Files.newBufferedWriter(tableFile.toPath(), java.nio.file.StandardOpenOption.APPEND)) {
                writer.write(String.join("\t", row));
                writer.newLine();
            } catch (IOException e) {
                return "[ERROR] Failed to write to table file";
            }
            return "[OK] Row inserted";
        }

        // 5. SELECT <WildAttribList> FROM <TableName> [WHERE <Condition>];
        Pattern selectPattern = Pattern.compile("(?i)^SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+))?\\s*;$");
        m = selectPattern.matcher(command);
        if (m.matches()) {
            String selectColumns = m.group(1).trim();
            String tableName = m.group(2).toLowerCase();
            String condition = (m.groupCount() >= 3) ? m.group(3) : null;
            if (currentDatabaseName == null) {
                return "[ERROR] No database selected";
            }
            Database db = databases.get(currentDatabaseName);
            if (!db.tables.containsKey(tableName)) {
                return "[ERROR] Table does not exist";
            }
            Table table = db.tables.get(tableName);
            List<Integer> colIndices = new ArrayList<>();
            List<String> headerOutput = new ArrayList<>();
            if (selectColumns.equals("*")) {
                for (int i = 0; i < table.columns.size(); i++) {
                    colIndices.add(i);
                    headerOutput.add(table.columns.get(i));
                }
            } else {
                String[] cols = selectColumns.split(",");
                for (String col : cols) {
                    String trimmed = col.trim();
                    int index = table.columns.indexOf(trimmed);
                    if (index == -1) {
                        return "[ERROR] Column " + trimmed + " does not exist";
                    }
                    colIndices.add(index);
                    headerOutput.add(trimmed);
                }
            }
            // 对数据行进行条件过滤（目前仅支持简单的单条件：<AttributeName> <Comparator> <Value>）
            List<List<String>> resultRows = new ArrayList<>();
            for (List<String> row : table.rows) {
                if (condition == null || condition.trim().isEmpty()) {
                    resultRows.add(row);
                } else {
                    Pattern condPattern = Pattern.compile("(?i)^(\\w+)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)$");
                    Matcher condMatcher = condPattern.matcher(condition.trim());
                    if (!condMatcher.matches()) {
                        return "[ERROR] Invalid condition";
                    }
                    String condColumn = condMatcher.group(1);
                    String comparator = condMatcher.group(2);
                    String condValue = condMatcher.group(3).trim();
                    if (condValue.startsWith("'") && condValue.endsWith("'") && condValue.length() >= 2) {
                        condValue = condValue.substring(1, condValue.length() - 1);
                    }
                    int colIndex = table.columns.indexOf(condColumn);
                    if (colIndex == -1) {
                        return "[ERROR] Column " + condColumn + " does not exist";
                    }
                    String cellValue = row.get(colIndex);
                    boolean conditionMatches = false;
                    // 尝试进行数字比较
                    Double cellNum = null, condNum = null;
                    try {
                        cellNum = Double.parseDouble(cellValue);
                        condNum = Double.parseDouble(condValue);
                    } catch (NumberFormatException e) {
                        // 非数字则保持 null
                    }
                    switch (comparator) {
                        case "==":
                            conditionMatches = cellValue.equals(condValue);
                            break;
                        case "!=":
                            conditionMatches = !cellValue.equals(condValue);
                            break;
                        case ">":
                            if (cellNum != null && condNum != null)
                                conditionMatches = cellNum > condNum;
                            break;
                        case "<":
                            if (cellNum != null && condNum != null)
                                conditionMatches = cellNum < condNum;
                            break;
                        case ">=":
                            if (cellNum != null && condNum != null)
                                conditionMatches = cellNum >= condNum;
                            break;
                        case "<=":
                            if (cellNum != null && condNum != null)
                                conditionMatches = cellNum <= condNum;
                            break;
                        case "LIKE":
                            conditionMatches = cellValue.contains(condValue);
                            break;
                        default:
                            return "[ERROR] Unknown comparator";
                    }
                    if (conditionMatches) {
                        resultRows.add(row);
                    }
                }
            }
            // 构造输出（第一行为列标题，其后每行一条记录，各列之间使用制表符分隔）
            StringBuilder output = new StringBuilder("[OK]\n");
            output.append(String.join("\t", headerOutput));
            for (List<String> row : resultRows) {
                output.append("\n");
                List<String> rowOutput = new ArrayList<>();
                for (Integer idx : colIndices) {
                    rowOutput.add(row.get(idx));
                }
                output.append(String.join("\t", rowOutput));
            }
            return output.toString();
        }

        return "[ERROR] Unrecognized command";
    }

    // === Methods below handle networking aspects of the project - you will not need to change these ! ===

    public void blockingListenOn(int portNumber) throws IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(portNumber)) {
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

    private void blockingHandleConnection(java.net.ServerSocket serverSocket) throws IOException {
        try (java.net.Socket s = serverSocket.accept();
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

    // 内部类：Database 表示一个数据库
    private class Database {
        String name;
        // 表名（小写）到 Table 的映射
        Map<String, Table> tables = new HashMap<>();

        Database(String name) {
            this.name = name;
            // 从文件系统加载当前数据库目录下的所有表
            File dbDir = new File(storageFolderPath, name);
            if (dbDir.exists() && dbDir.isDirectory()) {
                File[] files = dbDir.listFiles((dir, filename) -> filename.endsWith(".txt"));
                if (files != null) {
                    for (File file : files) {
                        String tableName = file.getName().substring(0, file.getName().length() - 4).toLowerCase();
                        try {
                            List<String> lines = Files.readAllLines(file.toPath());
                            if (lines.size() > 0) {
                                String headerLine = lines.get(0);
                                String[] cols = headerLine.split("\t");
                                List<String> columns = new ArrayList<>();
                                for (String col : cols) {
                                    columns.add(col);
                                }
                                Table table = new Table(tableName, columns);
                                for (int i = 1; i < lines.size(); i++) {
                                    String line = lines.get(i);
                                    String[] cells = line.split("\t");
                                    List<String> row = new ArrayList<>();
                                    for (String cell : cells) {
                                        row.add(cell);
                                    }
                                    table.rows.add(row);
                                }
                                // 计算下一个 id（不重复使用已删除行的 id）
                                int maxId = 0;
                                for (List<String> row : table.rows) {
                                    try {
                                        int id = Integer.parseInt(row.get(0));
                                        if (id > maxId) {
                                            maxId = id;
                                        }
                                    } catch (NumberFormatException e) {
                                        // 忽略非数字 id
                                    }
                                }
                                table.nextId = maxId + 1;
                                tables.put(tableName, table);
                            }
                        } catch (IOException e) {
                            // 忽略加载错误
                        }
                    }
                }
            }
        }
    }

    // 内部类：Table 表示一个数据表
    private class Table {
        String name;
        List<String> columns;
        int nextId;
        List<List<String>> rows;

        Table(String name, List<String> columns) {
            this.name = name;
            this.columns = columns;
            this.nextId = 1;
            this.rows = new ArrayList<>();
        }
    }
}
