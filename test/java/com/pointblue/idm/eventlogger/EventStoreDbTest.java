package com.pointblue.idm.eventlogger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Writes events to a real PostgreSQL and reads them back.
 * Runs only when EVENTLOGGER_TEST_JDBC_URL is set (with EVENTLOGGER_TEST_USER and
 * EVENTLOGGER_TEST_PASSWORD). Each test gets a throwaway schema.
 */
class EventStoreDbTest {

    static final String SCHEMA = "eventlogger_test";
    static final String TABLE = SCHEMA + ".dxmlevent";
    static final String LOGGER_DN = "\\TREE\\system\\ds\\EventLogger";

    static String url;
    static Connection conn;

    @BeforeAll
    static void connect() throws Exception {
        url = System.getenv("EVENTLOGGER_TEST_JDBC_URL");
        assumeTrue(url != null && !url.isEmpty(), "EVENTLOGGER_TEST_JDBC_URL not set");
        conn = DriverManager.getConnection(url, System.getenv("EVENTLOGGER_TEST_USER"),
                System.getenv("EVENTLOGGER_TEST_PASSWORD"));
    }

    @AfterAll
    static void cleanUp() throws Exception {
        if (conn != null) {
            exec("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
            conn.close();
        }
    }

    @BeforeEach
    void freshSchema() throws Exception {
        exec("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE; CREATE SCHEMA " + SCHEMA);
    }

    static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** Runs a script with the test schema first on the search path. */
    static void runScript(String path) throws Exception {
        exec("SET search_path TO " + SCHEMA);
        try {
            exec(new String(Files.readAllBytes(Paths.get(path)), "UTF-8"));
        } finally {
            exec("SET search_path TO DEFAULT");
        }
    }

    static List<List<String>> rows(String sql) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    row.add(rs.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    static EventRecord addEvent() throws Exception {
        return EventRecords.prepare(PasswordRedactorTest.resource("add-with-passwords.xml"));
    }

    @Test
    void storedAddEventCarriesOnlyTheMarker() throws Exception {
        runScript("sql/CREATE jsonEvent.sql");
        EventRecords.insert(conn, TABLE, addEvent(), true, LOGGER_DN);

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT eventjson::text AS j, xmlevent FROM " + TABLE + " WHERE eventid = ?")) {
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

    @Test
    void driverRowLeavesPolicyColumnsNullAndRejectsDuplicates() throws Exception {
        runScript("sql/CREATE jsonEvent.sql");
        EventRecords.insert(conn, TABLE, addEvent(), true, LOGGER_DN);

        assertEquals(List.of(List.of(LOGGER_DN, "null", "null", "null")),
                rows("SELECT srcdriver, coalesce(channel,'null'), coalesce(policy,'null'), coalesce(stage,'null') FROM " + TABLE));

        SQLException dup = assertThrows(SQLException.class,
                () -> EventRecords.insert(conn, TABLE, addEvent(), true, LOGGER_DN));
        assertEquals("23505", dup.getSQLState());
    }

    @Test
    void policyLoggerStoresChannelPolicyAndStage() throws Exception {
        runScript("sql/CREATE jsonEvent.sql");
        String xml = PasswordRedactorTest.resource("add-with-passwords.xml");
        URI jdbc = URI.create(url.substring("jdbc:".length()));
        PolicyLogger.register(LOGGER_DN, jdbc.getHost() + ":" + jdbc.getPort() + jdbc.getPath(),
                System.getenv("EVENTLOGGER_TEST_USER"), System.getenv("EVENTLOGGER_TEST_PASSWORD"), TABLE, true);
        try {
            String ad = "\\TREE\\system\\ds\\AD";
            // Same engine event: the driver's own row plus two policy stages
            EventRecords.insert(conn, TABLE, addEvent(), true, LOGGER_DN);
            assertTrue(PolicyLogger.logEvent(LOGGER_DN, ad, "sub", "AD-Sub-ETP", xml));
            assertTrue(PolicyLogger.logEvent(LOGGER_DN, ad, "subscriber", "AD-Sub-ETP", "output", xml));
            assertTrue(PolicyLogger.logEvent(LOGGER_DN, ad, "Pub", "\\TREE\\system\\ds\\AD\\Pub-ETP", "", xml));
            assertFalse(PolicyLogger.logEvent(LOGGER_DN, ad, "sideways", "X", xml), "unknown channel");
            assertFalse(PolicyLogger.logEvent(LOGGER_DN, ad, "sub", "X", "middle", xml), "unknown stage");
        } finally {
            PolicyLogger.unregister(LOGGER_DN);
        }

        assertEquals(List.of(
                        List.of("\\TREE\\system\\ds\\AD", "publisher", "\\TREE\\system\\ds\\AD\\Pub-ETP", "input"),
                        List.of("\\TREE\\system\\ds\\AD", "subscriber", "AD-Sub-ETP", "input"),
                        List.of("\\TREE\\system\\ds\\AD", "subscriber", "AD-Sub-ETP", "output"),
                        List.of(LOGGER_DN, "-", "-", "-")),
                rows("SELECT srcdriver, coalesce(channel,'-'), coalesce(policy,'-'), coalesce(stage,'-') FROM " + TABLE
                        + " WHERE eventid = '1714143050#7' ORDER BY srcdriver, channel, stage"));
        assertEquals(0, rows("SELECT 1 FROM " + TABLE + " WHERE xmlevent LIKE '%S3cret%'").size());
    }

    @Test
    void migrationUpgradesPreReleaseSchema() throws Exception {
        runScript("test/resources/sql/schema-pre-1.0.sql");
        exec("INSERT INTO " + TABLE + " (eventid, classname, srcdn, srcentryid, eventtype, eventjson, cachedtime, srcdriver) VALUES "
                + "('1#1','User','\\T\\u1','1','add','{}', now(), 'drv'),"
                + "('1#2','User','\\T\\u2','2','modify','{\"logged-by-policy\":\"P1\",\"logged-channel\":\"pub\"}', now(), 'ad')");

        runScript("sql/MIGRATE 2 policy columns.sql");
        runScript("sql/MIGRATE 2 policy columns.sql"); // re-runnable

        assertEquals(List.of(List.of("id")), rows(
                "SELECT a.attname FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)"
                        + " WHERE c.conrelid = '" + TABLE + "'::regclass AND c.contype = 'p'"));
        assertEquals(List.of(List.of("1#1", "-", "-", "-"), List.of("1#2", "publisher", "P1", "input")),
                rows("SELECT eventid, coalesce(channel,'-'), coalesce(policy,'-'), coalesce(stage,'-') FROM " + TABLE + " ORDER BY id"));

        // New rows go in after the upgrade, and the same event can be logged at a policy
        EventRecord record = addEvent();
        EventRecords.insert(conn, TABLE, record, true, LOGGER_DN);
        EventRecords.setPolicySource(record, "sub", "P2", "output");
        EventRecords.insert(conn, TABLE, record, true, "ad");
        assertEquals(4, rows("SELECT 1 FROM " + TABLE).size());
    }
}
