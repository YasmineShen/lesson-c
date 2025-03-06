package edu.uob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.String;

import static org.junit.jupiter.api.Assertions.*;

public class FinalTests {
    private DBServer server;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    // Random name generator - useful for testing "bare earth" queries (i.e. where tables don't previously exist)
    private String generateRandomName() {
        StringBuilder randomName = new StringBuilder();
        for(int i=0; i<10 ;i++) randomName.append((char)( 97 + (Math.random() * 25.0)));
        return randomName.toString();
    }

    private String sendCommandToServer(String command) {
        // Try to send a command to the server - this call will timeout if it takes too long (in case the server enters an infinite loop)
        return assertTimeoutPreemptively(Duration.ofMillis(1000), () -> { return server.handleCommand(command);},
                "Server took too long to respond (probably stuck in an infinite loop)");
    }

    // A basic test that creates a database, creates a table, inserts some test data, then queries it.
    // It then checks the response to see that a couple of the entries in the table are returned as expected
    @Test
    public void testDoc() {
        String randomName = generateRandomName();
        String response;
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");

        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");

        sendCommandToServer("CREATE TABLE coursework (task, submission);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO', 3);");
        sendCommandToServer("INSERT INTO coursework VALUES ('DB', 1);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO', 4);");
        sendCommandToServer("INSERT INTO coursework VALUES ('STAG', 2);");

        response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Simon"), "An attempt was made to add Simon to the table, but they were not returned by SELECT *");
        assertTrue(response.contains("Chris"), "An attempt was made to add Chris to the table, but they were not returned by SELECT *");

        response = sendCommandToServer("JOIN coursework AND marks ON submission AND id;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("OXO\tRob\t35\tFALSE"));
        assertTrue(response.contains("DB\tSimon\t65\tTRUE"));
        assertTrue(response.contains("OXO\tChris\t20\tFALSE"));
        assertTrue(response.contains("STAG\tSion\t55\tTRUE"));

        sendCommandToServer("UPDATE marks SET mark = 38 WHERE name == 'Chris';");
        response = sendCommandToServer("SELECT * FROM marks WHERE name == 'Chris';");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Chris\t38\tFALSE"));


        sendCommandToServer("DELETE FROM marks WHERE name == 'Sion';");
        response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertFalse(response.contains("Sion"));

        response = sendCommandToServer("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Chris\t38\tFALSE"));

        response = sendCommandToServer("SELECT * FROM marks WHERE name LIKE 'i';");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Simon\t65\tTRUE"));
        assertTrue(response.contains("Chris\t38\tFALSE"));

        sendCommandToServer("ALTER TABLE marks ADD age;");
        response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("id\tname\tmark\tpass\tage"));

        sendCommandToServer("ALTER TABLE marks DROP pass;");
        sendCommandToServer("UPDATE marks SET age = 35 WHERE name == 'Simon';");
        response = sendCommandToServer("SELECT * FROM marks;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("id\tname\tmark\tage"));
        assertTrue(response.contains("1\tSimon\t65\t35"));

        response = sendCommandToServer("SELECT * FROM crew;");
        assertFalse(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");

        response = sendCommandToServer("SELECT height FROM marks WHERE name == 'Chris';");
        assertFalse(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");

        response = sendCommandToServer("DROP TABLE marks;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");

        response = sendCommandToServer("DROP DATABASE " + randomName + ";");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");

    }

    @Test
    public void testDifficult() {
        String randomName = generateRandomName();
        String response;
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");

        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");

        sendCommandToServer("CREATE TABLE coursework (task, submission);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO', 3);");
        sendCommandToServer("INSERT INTO coursework VALUES ('DB', 1);");
        sendCommandToServer("INSERT INTO coursework VALUES ('OXO', 4);");
        sendCommandToServer("INSERT INTO coursework VALUES ('STAG', 2);");

        sendCommandToServer("INSERT INTO coursework VALUES ('STAG); drop database " + randomName + ";', 2);");

        sendCommandToServer("ALTER TABLE marks DROP pass;");
        response = sendCommandToServer("SELECT * FROM marks where pass == NULL;");
        assertFalse(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");

        response = sendCommandToServer("update marks set id=3 where mark==65;");
        assertFalse(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");

        response = sendCommandToServer("ALTER TABLE marks DROP id;");
        assertFalse(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");

    }

    @Test
    public void testDifficult2() {
        String randomName = generateRandomName();
        String response;
        sendCommandToServer("CREATE DATABASE " + randomName + ";");
        sendCommandToServer("USE " + randomName + ";");
        sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        sendCommandToServer("INSERT INTO marks VALUES('%.{$abc}abc', 65.1, TRUE);");
        sendCommandToServer("USE " + randomName + ";");
        response = sendCommandToServer("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);");
        response = sendCommandToServer("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);");
    }
}