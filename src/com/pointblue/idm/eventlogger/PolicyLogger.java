package com.pointblue.idm.eventlogger;

import com.novell.nds.dirxml.driver.Trace;

import java.sql.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event logger with a static registry for use from DirXML policies and ECMAScript.
 * <p>
 * Each running EventLoggerDriver registers itself during {@code init()} and unregisters
 * during {@code shutdown()}, keyed by the driver's DN. Policy code can then log events
 * by referencing a driver DN without needing database credentials.
 * <p>
 * <h3>Usage from ECMAScript in a DirXML policy:</h3>
 * <pre>
 *   var PolicyLogger = Packages.com.pointblue.idm.eventlogger.PolicyLogger;
 *   var eventLoggerDN = "\\TREENAME\\system\\driverset\\EventLogger";
 *   var thisDriverDN  = "\\TREENAME\\system\\driverset\\ActiveDirectory";
 *   var xmlString = XPATH.get("/");
 *   PolicyLogger.logEvent(eventLoggerDN, thisDriverDN, "sub", "AD-Sub-ETP", xmlString);
 * </pre>
 * <p>
 * <h3>Direct instantiation (without registry):</h3>
 * <pre>
 *   PolicyLogger logger = new PolicyLogger("localhost:5432/idmEvent", "postgres", "password", null);
 *   try {
 *       logger.writeEventToDB(eventJSON, xmlDoc, true);
 *   } finally {
 *       logger.close();
 *   }
 * </pre>
 * <p>
 * <h3>Connection pooling:</h3>
 * Each registered PolicyLogger maintains a pool of JDBC connections to support concurrent
 * callers from multiple driver policies without blocking. Connections are borrowed from the
 * pool, used for a single INSERT, and returned. If the pool is exhausted, callers block
 * briefly until a connection becomes available.
 *
 * @see EventLoggerDriver
 */
public class PolicyLogger {

    /** Registry of active PolicyLogger instances keyed by driver DN. */
    private static final ConcurrentHashMap<String, PolicyLogger> registry = new ConcurrentHashMap<>();

    /*
     * Connection pool size. 10 connections is sufficient for typical IDM deployments where
     * a handful of drivers may call logEvent() concurrently. Each connection is lightweight
     * (~1-2 MB of PostgreSQL backend memory). If you have more than 10 drivers logging
     * simultaneously, increase this value. Reduce it if database connections are scarce.
     *
     * To change, modify POOL_SIZE and rebuild the JAR.
     */
    private static final int POOL_SIZE = 10;

    /** Trace instance for diagnostic output. */
    private final Trace tracer = new Trace("PolicyLogger");

    /** Pool of reusable JDBC connections. Threads borrow and return connections. */
    private final BlockingQueue<Connection> connectionPool = new ArrayBlockingQueue<>(POOL_SIZE);

    /** JDBC connection URL (e.g. {@code jdbc:postgresql://localhost:5432/idmEvent}). */
    private final String dbUrl;

    /** PostgreSQL username. */
    private final String dbUser;

    /** PostgreSQL password. */
    private final String dbPassword;

    /** Target table name for event inserts. */
    private final String tableName;

    /** Whether to store the original XML alongside JSON. */
    private final boolean logXML;

    /** The DN of the driver this logger is associated with, stored in each event row. */
    private final String driverDN;

    /** Whether this logger has been closed. */
    private volatile boolean closed = false;

    // --- Static Registry API ---

    /**
     * Registers a PolicyLogger instance for a driver DN.
     * Called by {@link EventLoggerDriver#init} when the driver starts.
     *
     * @param driverDN  the fully qualified DN of the driver (e.g. "\\TREENAME\\system\\driverset\\EventLogger")
     * @param dbPath    the PostgreSQL host:port/database (e.g. "localhost:5432/idmEvent")
     * @param user      the PostgreSQL username
     * @param password  the PostgreSQL password
     * @param tableName the target table name, or {@code null} for the default ("public.dxmlevent")
     * @param logXML    whether to store the original XML document
     */
    public static void register(String driverDN, String dbPath, String user, String password, String tableName, boolean logXML) {
        PolicyLogger logger = new PolicyLogger(dbPath, user, password, tableName, logXML, driverDN);
        PolicyLogger old = registry.put(driverDN, logger);
        if (old != null) {
            old.close();
        }
        logger.tracer.trace("PolicyLogger registered for driver: " + driverDN, 0);
    }

    /**
     * Unregisters and closes the PolicyLogger for a driver DN.
     * Called by {@link EventLoggerDriver#shutdown} when the driver stops.
     *
     * @param driverDN the driver DN to unregister
     */
    public static void unregister(String driverDN) {
        PolicyLogger logger = registry.remove(driverDN);
        if (logger != null) {
            logger.close();
            logger.tracer.trace("PolicyLogger unregistered for driver: " + driverDN, 0);
        }
    }

    /**
     * Logs an XDS XML event using the registered logger for the specified driver.
     * <p>
     * This is the primary entry point for ECMAScript policy code. The XML string
     * is parsed to determine the event type, converted to JSON, and written to the
     * database. No database credentials are needed — they come from the registered driver.
     * <p>
     * Example ECMAScript usage:
     * <pre>
     *   var PolicyLogger = Packages.com.pointblue.idm.eventlogger.PolicyLogger;
     *   PolicyLogger.logEvent(
     *       "\\TREENAME\\system\\driverset\\EventLogger",
     *       "\\TREENAME\\system\\driverset\\ActiveDirectory",
     *       "sub",           // channel: "sub" or "pub"
     *       "AD-Sub-ETP",    // policy name for tracing
     *       XPATH.get("/")   // current operation document
     *   );
     * </pre>
     *
     * @param eventLoggerDN the DN of the EventLoggerDriver to log through
     * @param srcDriverDN   the DN of the driver whose policy is calling this method (stored in {@code srcdriver} column)
     * @param channel       the channel name ("sub" or "pub") for context in the log entry
     * @param policyDN      the name or DN of the calling policy (for tracing)
     * @param xmlString     the XDS XML document as a string
     * @return {@code true} if the event was logged successfully, {@code false} on error
     */
    public static boolean logEvent(String eventLoggerDN, String srcDriverDN, String channel, String policyDN, String xmlString) {
        return logEvent(eventLoggerDN, srcDriverDN, channel, policyDN, "input", xmlString);
    }

    /**
     * Logs an XDS XML event, recording whether the document is the policy's input or output.
     * <p>
     * Stores {@code channel}, {@code policy} and {@code stage} in their own columns.
     * <pre>
     *   PolicyLogger.logEvent(eventLoggerDN, thisDriverDN, "sub", "AD-Sub-ETP", "output", XPATH.get("/"));
     * </pre>
     *
     * @param eventLoggerDN the DN of the EventLoggerDriver to log through
     * @param srcDriverDN   the DN of the driver whose policy is calling this method (stored in {@code srcdriver})
     * @param channel       "subscriber" or "publisher" ("sub" and "pub" are accepted)
     * @param policyDN      the name or DN of the calling policy, stored as given in {@code policy}
     * @param stage         "input" or "output" (null or empty means "input")
     * @param xmlString     the XDS XML document as a string
     * @return {@code true} if the event was logged successfully, {@code false} on error
     */
    public static boolean logEvent(String eventLoggerDN, String srcDriverDN, String channel, String policyDN, String stage, String xmlString) {
        Trace trace = new Trace("PolicyLogger");

        PolicyLogger logger = registry.get(eventLoggerDN);
        if (logger == null) {
            trace.trace("No PolicyLogger registered for driver: " + eventLoggerDN, 0);
            return false;
        }

        try {
            EventRecord record = EventRecords.prepare(xmlString);
            EventRecords.setPolicySource(record, channel, policyDN, stage);

            // Also in the JSON, for readers of rows written before the columns existed
            record.json.put("logged-by-policy", policyDN);
            record.json.put("logged-channel", channel);

            // Write to DB with the calling driver's DN as the source
            logger.write(record, srcDriverDN);
            trace.trace("PolicyLogger.logEvent: Logged " + record.eventType() + " event from policy " + policyDN + " on driver " + srcDriverDN, 3);
            return true;

        } catch (IllegalArgumentException e) {
            trace.trace("PolicyLogger.logEvent: " + e.getMessage() + " (policy " + policyDN + ")", 1);
            return false;
        } catch (Exception e) {
            trace.trace("PolicyLogger.logEvent error from policy " + policyDN + ": " + e.getMessage(), 0);
            return false;
        }
    }

    /**
     * Returns the number of currently registered loggers. Useful for diagnostics.
     *
     * @return the count of registered PolicyLogger instances
     */
    public static int getRegisteredCount() {
        return registry.size();
    }

    /**
     * Checks whether a logger is registered for the given driver DN.
     *
     * @param driverDN the driver DN to check
     * @return {@code true} if a logger is registered for this DN
     */
    public static boolean isRegistered(String driverDN) {
        return registry.containsKey(driverDN);
    }

    // --- Instance API ---

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
        this(dbPath, user, password, tableName, true, null);
    }

    /**
     * Constructs a PolicyLogger with the given database connection parameters.
     *
     * @param dbPath    the PostgreSQL host, port, and database (e.g. "localhost:5432/idmEvent")
     * @param user      the PostgreSQL username
     * @param password  the PostgreSQL password
     * @param tableName the target table name, or {@code null} to use the default ("public.dxmlevent")
     * @param logXML    whether to store the original XML document
     * @param driverDN  the DN of the source driver, or {@code null} if not associated with a driver
     */
    public PolicyLogger(String dbPath, String user, String password, String tableName, boolean logXML, String driverDN) {
        this.dbUrl = "jdbc:postgresql://" + dbPath;
        this.dbUser = user;
        this.dbPassword = password;
        this.tableName = tableName != null ? tableName : "public.dxmlevent";
        this.logXML = logXML;
        this.driverDN = driverDN;
    }

    /**
     * Borrows a connection from the pool, creating a new one if the pool is empty
     * and hasn't reached capacity. Validates the connection before returning it.
     *
     * @return a valid JDBC connection
     * @throws SQLException if a connection cannot be obtained
     */
    private Connection borrowConnection() throws SQLException {
        if (closed) {
            throw new SQLException("PolicyLogger is closed");
        }

        // Try to get an existing connection from the pool (non-blocking)
        Connection conn = connectionPool.poll();

        if (conn != null) {
            // Validate the pooled connection
            try {
                if (!conn.isClosed() && conn.isValid(2)) {
                    return conn;
                }
            } catch (SQLException e) {
                // Connection is bad, close it and fall through to create a new one
                try { conn.close(); } catch (SQLException ignored) {}
            }
        }

        // Create a new connection
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    /**
     * Returns a connection to the pool. If the pool is full, the connection is closed.
     *
     * @param conn the connection to return
     */
    private void returnConnection(Connection conn) {
        if (conn == null) return;

        if (closed) {
            try { conn.close(); } catch (SQLException ignored) {}
            return;
        }

        // Try to return to the pool; if full, close the connection
        if (!connectionPool.offer(conn)) {
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    /**
     * Closes all connections in the pool and marks this logger as closed.
     * Safe to call multiple times.
     */
    public void close() {
        closed = true;
        Connection conn;
        while ((conn = connectionPool.poll()) != null) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                tracer.trace("Error closing pooled connection: " + e.getMessage(), 1);
            }
        }
    }

    /**
     * Writes a prepared record using a pooled connection.
     * <p>
     * Borrows a connection from the pool, executes the INSERT, and returns the
     * connection. Each caller gets its own connection, so no synchronization is needed.
     *
     * @param record      the record to store
     * @param srcDriverDN the DN of the driver that originated the event
     * @throws SQLException if the database insert fails
     */
    private void write(EventRecord record, String srcDriverDN) throws SQLException {
        Connection conn = borrowConnection();
        try {
            EventRecords.insert(conn, tableName, record, logXML, srcDriverDN);
            returnConnection(conn);
        } catch (SQLException e) {
            // Connection may be bad — close it instead of returning to pool
            try { conn.close(); } catch (SQLException ignored) {}
            throw e;
        }
    }
}
