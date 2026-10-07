package com.pointblue.idm.eventlogger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Writes events to a real PostgreSQL through {@link EventRecords#insert} and reads them back.
 * Runs only when EVENTLOGGER_TEST_JDBC_URL is set (with EVENTLOGGER_TEST_USER and
 * EVENTLOGGER_TEST_PASSWORD); applies sql/CREATE jsonEvent.sql in a throwaway schema.
 */
class EventStoreDbTest {

    static final String SCHEMA = "eventlogger_it";
    static Connection conn;

    @BeforeAll
    static void connect() throws Exception {
        String url = System.getenv("EVENTLOGGER_TEST_JDBC_URL");
        assumeTrue(url != null && !url.isEmpty(), "EVENTLOGGER_TEST_JDBC_URL not set");
        conn = DriverManager.getConnection(url, System.getenv("EVENTLOGGER_TEST_USER"),
                System.getenv("EVENTLOGGER_TEST_PASSWORD"));
        try (Statement st = conn.createStatement()) {
            st.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE; CREATE SCHEMA " + SCHEMA
                    + "; SET search_path TO " + SCHEMA);
            st.execute(new String(Files.readAllBytes(Paths.get("sql", "CREATE jsonEvent.sql")), "UTF-8"));
        }
    }

    @AfterAll
    static void cleanUp() throws Exception {
        if (conn != null) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
            }
            conn.close();
        }
    }

    @Test
    void storedAddEventCarriesOnlyTheMarker() throws Exception {
        EventRecord record = EventRecords.prepare(PasswordRedactorTest.resource("add-with-passwords.xml"));
        EventRecords.insert(conn, SCHEMA + ".dxmlevent", record, true, "\\TREE\\system\\ds\\EventLogger");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT eventjson::text AS j, xmlevent FROM " + SCHEMA + ".dxmlevent WHERE eventid = ?")) {
            ps.setString(1, "1714143050#7");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                for (String stored : new String[]{rs.getString("j"), rs.getString("xmlevent")}) {
                    assertFalse(stored.contains("S3cret"), "password stored: " + stored);
                    assertTrue(stored.contains(PasswordRedactor.MARKER));
                }
            }
        }
    }
}
