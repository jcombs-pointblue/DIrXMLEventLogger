package com.pointblue.idm.eventlogger;

import com.novell.nds.dirxml.driver.Trace;
import com.novell.nds.dirxml.driver.XmlDocument;
import com.pointblue.idm.eventlogger.json.JSONObject;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.time.Instant;

/**
 * Provides logging capabilities for individual policy input/output documents.
 * Can be called by other drivers to log their policy execution data.
 */
public class PolicyLogger {

    private final Trace tracer = new Trace("PolicyLogger");
    private Connection dbConnection = null;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String tableName;

    public PolicyLogger(String dbPath, String user, String password, String tableName) {
        this.dbUrl = "jdbc:postgresql://" + dbPath;
        this.dbUser = user;
        this.dbPassword = password;
        this.tableName = tableName != null ? tableName : "public.dxmlevent";
    }

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