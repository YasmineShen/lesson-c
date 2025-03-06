package edu.uob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBServer {
    private static final char END_OF_TRANSMISSION = 4;
    private final String storageFolderPath;
    private String currentDatabaseName;
    private final Map<String, Database> databases;

    public static void main(String[] args) throws IOException {
        var server = new DBServer();
        server.blockingListenOn(8888);
    }

    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        databases = new HashMap<>();
        try {
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch (IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
        loadDatabases();
    }

    private void loadDatabases() {
        var storageFolder = new File(storageFolderPath);
        var dbDirs = storageFolder.listFiles(File::isDirectory);
        if (dbDirs != null) {
            for (var dbDir : dbDirs) {
                var dbName = dbDir.getName().toLowerCase();
                var db = new Database(dbName);
                databases.put(dbName, db);
            }
        }
    }

    public String handleCommand(String command) {
        command = command.trim();
        if (!command.endsWith(";")) {
            return "[ERROR] Command must end with a semicolon";
        }

        // 1. CREATE DATABASE <DatabaseName>;
        var createDatabasePattern = Pattern.compile("(?i)^CREATE\\s+DATABASE\\s+(\\w+)\\s*;$");
        var m = createDatabasePattern.matcher(command);
        if (m.matches()) {
            var dbName = m.group(1).toLowerCase();
            if (databases.containsKey(dbName)) {
                return "[ERROR] Database already exists";
            }
            var dbDir = new File(storageFolderPath, dbName);
            if (!dbDir.exists() && !dbDir.mkdirs()) {
                return "[ERROR] Could not create database directory";
            }
            var db = new Database(dbName);
            databases.put(dbName, db);
            return "[OK] Database created";
        }

        // 2. USE <DatabaseName>;
        var usePattern = Pattern.compile("(?i)^USE\\s+(\\w+)\\s*;$");
        m = usePattern.matcher(command);
        if (m.matches()) {
            var dbName = m.group(1).toLowerCase();
            if (!databases.containsKey(dbName)) {
                return "[ERROR] Database does not exist";
            }
            currentDatabaseName = dbName;
            return "[OK] Using database " + dbName;
        }

        // 3. CREATE TABLE <TableName> [(<AttributeList>)];
        var createTablePattern = Pattern.compile("(?i)^CREATE\\s+TABLE\\s+(\\w+)(?:\\s*\\(([^)]+)\\))?\\s*;$");
        m = createTablePattern.matcher(command);
        if (m.matches()) {
            var tableName = m.group(1).toLowerCase();
            if (currentDatabaseName == null) {
                return "[ERROR] No database selected";
            }
            var db = databases.get(currentDatabaseName);
            if (db.tables.containsKey(tableName)) {
                return "[ERROR] Table already exists";
            }
            var columns = new ArrayList<String>();
            // 默认第一列固定为 id
            columns.add("id");
            var attrList = m.group(2);
            if (attrList != null) {
                var attrs = attrList.split(",");
                for (var attr : attrs) {
                    var trimmed = attr.trim();
                    if (trimmed.equalsIgnoreCase("id")) {
                        return "[ERROR] Cannot use reserved attribute name 'id'";
                    }
                    columns.add(trimmed);
                }
            }
            var table = new Table(tableName, columns);
            db.tables.put(tableName, table);
            var tableFile = new File(storageFolderPath + File.separator + currentDatabaseName, tableName + ".txt");
            try (var writer = Files.newBufferedWriter(tableFile.toPath())) {
                writer.write(String.join("\t", columns));
                writer.newLine();
            } catch (IOException e) {
                return "[ERROR] Failed to create table file";
            }
            return "[OK] Table created";
        }

        // 4. INSERT INTO <TableName> VALUES (<ValueList>);
        var insertPattern = Pattern.compile("(?i)^INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s*\\((.+)\\)\\s*;$");
        m = insertPattern.matcher(command);
        if (m.matches()) {
            var tableName = m.group(1).toLowerCase();
            if (currentDatabaseName == null) {
                return "[ERROR] No database selected";
            }
            var db = databases.get(currentDatabaseName);
            if (!db.tables.containsKey(tableName)) {
                return "[ERROR] Table does not exist";
            }
            var table = db.tables.get(tableName);
            var valuesPart = m.group(2);
            var valueTokens = valuesPart.split(",");
            var values = new ArrayList<String>();
            for (var token : valueTokens) {
                var val = token.trim();
                if (val.startsWith("'") && val.endsWith("'") && val.length() >= 2) {
                    val = val.substring(1, val.length() - 1);
                }
                values.add(val);
            }
            if (values.size() != table.columns.size() - 1) {
                return "[ERROR] Incorrect number of values";
            }
            var idStr = String.valueOf(table.nextId);
            table.nextId++;
            var row = new ArrayList<String>();
            row.add(idStr);
            row.addAll(values);
            table.rows.add(row);
            var tableFile = new File(storageFolderPath + File.separator + currentDatabaseName, tableName + ".txt");
            try (var writer = Files.newBufferedWriter(tableFile.toPath(), StandardOpenOption.APPEND)) {
                writer.write(String.join("\t", row));
                writer.newLine();
            } catch (IOException e) {
                return "[ERROR] Failed to write to table file";
            }
            return "[OK] Row inserted";
        }

        // 5. SELECT <WildAttribList> FROM <TableName> [WHERE <Condition>];
        var selectPattern = Pattern.compile("(?i)^SELECT\\s+(.+?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+))?\\s*;$");
        m = selectPattern.matcher(command);
        if (m.matches()) {
            var selectColumns = m.group(1).trim();
            var tableName = m.group(2).toLowerCase();
            var condition = (m.groupCount() >= 3) ? m.group(3) : null;
            if (currentDatabaseName == null) {
                return "[ERROR] No database selected";
            }
            var db = databases.get(currentDatabaseName);
            if (!db.tables.containsKey(tableName)) {
                return "[ERROR] Table does not exist";
            }
            var table = db.tables.get(tableName);
            var colIndices = new ArrayList<Integer>();
            var headerOutput = new ArrayList<String>();
            if (selectColumns.equals("*")) {
                for (int i = 0; i < table.columns.size(); i++) {
                    colIndices.add(i);
                    headerOutput.add(table.columns.get(i));
                }
            } else {
                var cols = selectColumns.split(",");
                for (var col : cols) {
                    var trimmed = col.trim();
                    var index = table.columns.indexOf(trimmed);
                    if (index == -1) {
                        return "[ERROR] Column " + trimmed + " does not exist";
                    }
                    colIndices.add(index);
                    headerOutput.add(trimmed);
                }
            }
            var resultRows = new ArrayList<List<String>>();
            for (var row : table.rows) {
                if (condition == null || condition.trim().isEmpty()) {
                    resultRows.add(row);
                } else {
                    var condPattern = Pattern.compile("(?i)^(\\w+)\\s*(==|>|<|>=|<=|!=|LIKE)\\s*(.+)$");
                    var condMatcher = condPattern.matcher(condition.trim());
                    if (!condMatcher.matches()) {
                        return "[ERROR] Invalid condition";
                    }
                    var condColumn = condMatcher.group(1);
                    var comparator = condMatcher.group(2);
                    var condValue = condMatcher.group(3).trim();
                    if (condValue.startsWith("'") && condValue.endsWith("'") && condValue.length() >= 2) {
                        condValue = condValue.substring(1, condValue.length() - 1);
                    }
                    var colIndex = table.columns.indexOf(condColumn);
                    if (colIndex == -1) {
                        return "[ERROR] Column " + condColumn + " does not exist";
                    }
                    var cellValue = row.get(colIndex);
                    boolean conditionMatches;
                    Double cellNum = null, condNum = null;
                    try {
                        cellNum = Double.parseDouble(cellValue);
                        condNum = Double.parseDouble(condValue);
                    } catch (NumberFormatException e) {
                        // leave as null if not a number
                    }
                    conditionMatches = switch (comparator) {
                        case "==" -> cellValue.equals(condValue);
                        case "!=" -> !cellValue.equals(condValue);
                        case ">" -> (cellNum != null && condNum != null) && cellNum > condNum;
                        case "<" -> (cellNum != null && condNum != null) && cellNum < condNum;
                        case ">=" -> (cellNum != null && condNum != null) && cellNum >= condNum;
                        case "<=" -> (cellNum != null && condNum != null) && cellNum <= condNum;
                        case "LIKE" -> cellValue.contains(condValue);
                        default -> {
                            yield false; // unknown comparator
                        }
                    };
                    if (conditionMatches) {
                        resultRows.add(row);
                    }
                }
            }
            var output = new StringBuilder("[OK]\n");
            output.append(String.join("\t", headerOutput));
            for (var row : resultRows) {
                output.append("\n");
                var rowOutput = new ArrayList<String>();
                for (var idx : colIndices) {
                    rowOutput.add(row.get(idx));
                }
                output.append(String.join("\t", rowOutput));
            }
            return output.toString();
        }
        return "[ERROR] Unrecognized command";
    }

    public void blockingListenOn(int portNumber) throws IOException {
        try (var s = new ServerSocket(portNumber)) {
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
        try (var s = serverSocket.accept();
             var reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
             var writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                var incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                var result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }

    private class Database {
        String name;
        Map<String, Table> tables = new HashMap<>();

        Database(String name) {
            this.name = name;
            var dbDir = new File(storageFolderPath, name);
            if (dbDir.exists() && dbDir.isDirectory()) {
                var files = dbDir.listFiles((dir, filename) -> filename.endsWith(".txt"));
                if (files != null) {
                    for (var file : files) {
                        var tableName = file.getName().substring(0, file.getName().length() - 4).toLowerCase();
                        try {
                            var lines = Files.readAllLines(file.toPath());
                            if (!lines.isEmpty()) {
                                var headerLine = lines.get(0);
                                var cols = headerLine.split("\t");
                                var columns = new ArrayList<String>();
                                for (var col : cols) {
                                    columns.add(col);
                                }
                                var table = new Table(tableName, columns);
                                for (int i = 1; i < lines.size(); i++) {
                                    var line = lines.get(i);
                                    var cells = line.split("\t");
                                    var row = new ArrayList<String>();
                                    for (var cell : cells) {
                                        row.add(cell);
                                    }
                                    table.rows.add(row);
                                }
                                int maxId = 0;
                                for (var row : table.rows) {
                                    try {
                                        var id = Integer.parseInt(row.get(0));
                                        if (id > maxId) {
                                            maxId = id;
                                        }
                                    } catch (NumberFormatException e) {
                                        // ignore non-numeric id
                                    }
                                }
                                table.nextId = maxId + 1;
                                tables.put(tableName, table);
                            }
                        } catch (IOException e) {
                            // ignore loading errors
                        }
                    }
                }
            }
        }
    }

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
