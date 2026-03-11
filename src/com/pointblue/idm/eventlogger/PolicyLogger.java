package com.pointblue.idm.eventlogger;

import com.novell.nds.dirxml.driver.Trace;
import com.novell.nds.dirxml.driver.XmlDocument;
import com.pointblue.idm.eventlogger.json.JSONObject;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.time.Instant;

/**
 * Standalone event logger for use by DirXML policy objects or other drivers.
 * <p>
 * Unlike {@link EventLoggerDriver} which captures events on its own subscriber channel,
 * PolicyLogger is designed to be instantiated and called from within policy code or
 * other driver implementations to log arbitrary events or policy input/output documents
 * to the same PostgreSQL database.
 * <p>
 * Usage example from a DirXML policy or driver:
 * <pre>
 *   PolicyLogger logger = new PolicyLogger("localhost:5432/idmEvent", "postgres", "password", null);
 *   try {
 *       logger.writeEventToDB(eventJSON, xmlDoc, true);
 *   } finally {
 *       logger.close();
 *   }
 * </pre>
 * <p>
 * Maintains a single reusable JDBC connection with automatic reconnection on failure.
 * Call {@link #close()} when the logger is no longer needed to release the database connection.
 *
 * @see EventLoggerDriver
 */
public class PolicyLogger {

    /** Trace instance for diagnostic output. */
    private final Trace tracer = new Trace("PolicyLogger");

    /** Reusable JDBC connection, validated before each use. */
    private Connection dbConnection = null;

    /** JDBC connection URL (e.g. {@code jdbc:postgresql://localhost:5432/idmEvent}). */
    private final String dbUrl;

    /** PostgreSQL username. */
    private final String dbUser;

    /** PostgreSQL password. */
    private final String dbPassword;

    /** Target table name for event inserts. */
    private final String tableName;

    /**
     * Constructs a PolicyLogger with the given database connection parameters.
     *
     * @param dbPath    the PostgreSQL host, port, and database (e.g. "localhost:5432/idmEvent").
     *                  This is prepended with "jdbc:postgresql://" to form the JDBC URL.
     * @param user      the PostgreSQL username
     * @param password  the PostgreSQL password
     * @param tableName the target table name, or {@code null} to use the default ("public.dxmlevent")
     */
    public PolicyLogger(String dbPath, String user, String password, String tableName) {
        this.dbUrl = "jdbc:postgresql://" + dbPath;
        this.dbUser = user;
        this.dbPassword = password;
        this.tableName = tableName != null ? tableName : "public.dxmlevent";
    }

    /**
     * Gets or creates a database connection, reusing the existing one if still valid.
     *
     * @return a valid JDBC connection
     * @throws SQLException if the connection cannot be established
     */
    private synchronized Connection getConnection() throws SQLException {
        if (dbConnection != null && !dbConnection.isClosed())
        {
            try
            {
                if (dbConnection.isValid(2))
                {
                    return dbConnection;
                }
            } catch (SQLException e)
            {
                tracer.trace("Existing connection invalid, creating new one", 2);
            }
        }
        dbConnection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
        return dbConnection;
    }

    /**
     * Closes the database connection if open. Safe to call multiple times.
     * Should be called when the PolicyLogger is no longer needed.
     */
    public synchronized void close() {
        if (dbConnection != null)
        {
            try
            {
                if (!dbConnection.isClosed())
                {
                    dbConnection.close();
                }
            } catch (SQLException e)
            {
                tracer.trace("Error closing PolicyLogger connection: " + e.getMessage(), 1);
            }
            dbConnection = null;
        }
    }

    /**
     * Writes an event to the database.
     * <p>
     * Extracts the event ID, class name, source DN, entry ID, event type, and timestamp
     * from the JSON object, then inserts them along with the full JSON payload and
     * (optionally) the original XML document into the configured table.
     * <p>
     * The JSON object must contain the following keys:
     * <ul>
     *   <li>{@code timestamp} — DirXML timestamp in "epoch#sequence" format (e.g. "1714143050#2")</li>
     *   <li>{@code event-id} — the unique event identifier</li>
     *   <li>{@code class-name} — the object class (e.g. "User")</li>
     *   <li>{@code src-dn} — the source distinguished name</li>
     *   <li>{@code src-entry-id} — the source entry GUID</li>
     *   <li>{@code event-type} — the event type (add, modify, delete, etc.)</li>
     * </ul>
     *
     * @param eventJSON the event data as a JSON object
     * @param doc       the original XDS XML document
     * @param logXML    {@code true} to store the XML document, {@code false} to store NULL
     * @throws SQLException if the database insert fails
     */
    public void writeEventToDB(JSONObject eventJSON, XmlDocument doc, boolean logXML) throws SQLException {
        Connection conn = getConnection();
        String sql = "INSERT INTO " + tableName + " (\"eventid\", \"classname\", \"srcdn\", \"srcentryid\", \"eventtype\", \"eventjson\", \"cachedtime\", \"xmlevent\") VALUES(?,?,?,?,?,?,?,?);";

        try (PreparedStatement pstmt = conn.prepareStatement(sql))
        {
            long epochSeconds = Long.parseLong(eventJSON.getString("timestamp").split("#")[0]);
            Timestamp timestamp = Timestamp.from(Instant.ofEpochSecond(epochSeconds));
            pstmt.setTimestamp(7, timestamp);
            pstmt.setString(1, eventJSON.getString("event-id"));
            pstmt.setString(2, eventJSON.getString("class-name"));
            pstmt.setString(3, eventJSON.getString("src-dn"));
            pstmt.setString(4, eventJSON.getString("src-entry-id"));
            pstmt.setString(5, eventJSON.getString("event-type"));

            PGobject jsonObject = new PGobject();
            jsonObject.setType("json");
            jsonObject.setValue(eventJSON.toString());
            pstmt.setObject(6, jsonObject);

            if (logXML)
            {
                pstmt.setString(8, doc.getDocumentString());
            } else
            {
                pstmt.setNull(8, Types.VARCHAR);
            }
            pstmt.executeUpdate();
        }
    }
}
