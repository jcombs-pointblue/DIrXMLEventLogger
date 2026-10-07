package com.pointblue.idm.eventlogger;

import com.novell.nds.dirxml.driver.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.sql.*;


/**
 * DirXML Event Logger driver for NetIQ Identity Manager.
 * <p>
 * Captures events from the subscriber channel (add, modify, delete, sync, rename, move),
 * converts them to JSON using the {@link BaseEventConverter} hierarchy, and writes both
 * the JSON and (optionally) the original XDS XML to a PostgreSQL database.
 * <p>
 * This driver implements all three shim interfaces ({@link DriverShim},
 * {@link SubscriptionShim}, {@link PublicationShim}) as well as {@link XmlQueryProcessor}.
 * The publication side provides heartbeat support only; no events are published.
 *
 * <h3>Driver Options</h3>
 * <ul>
 *   <li><b>storeXML</b> — set to "false" to skip storing the raw XML document (default: true)</li>
 *   <li><b>tableName</b> — override the target table name (default: "public.dxmlevent")</li>
 * </ul>
 *
 * <h3>Authentication</h3>
 * <ul>
 *   <li><b>Authentication ID</b> — PostgreSQL username</li>
 *   <li><b>Authentication context</b> — PostgreSQL connection string (e.g. localhost:5432/idmEvent)</li>
 *   <li><b>Application password</b> — PostgreSQL password</li>
 * </ul>
 *
 * <h3>Connection Management</h3>
 * A single JDBC connection is maintained and reused across events. If the connection
 * becomes invalid, it is automatically re-established. On repeated connection failures,
 * exponential backoff is applied (1s, 2s, 4s, ... up to 5 minutes) to avoid overwhelming
 * an unavailable database. The backoff resets on successful reconnection.
 *
 * @see BaseEventConverter
 * @see PolicyLogger
 */
public class EventLoggerDriver extends CommonImpl implements DriverShim, PublicationShim, SubscriptionShim, XmlQueryProcessor {

    /**
     * Descriptors for the parameters this driver reads from the
     * {@code <driver-options>} element of the init document.
     */
    public static final ShimParamDesc[] DRIVER_PARAMS
            = {
            new ShimParamDesc("storeXML", ShimParamDesc.STRING_TYPE, false),
            new ShimParamDesc("tableName", ShimParamDesc.STRING_TYPE, false)

    };

    /** Release version of this jar; kept in step with pom.xml (VersionTest checks it). */
    public static final String VERSION = "1.0.0";

    /** Minimum activation version required for this driver. */
    private static final String DRIVER_MIN_ACTIVATION_VERSION = "0";

    /** Monitor object used for publisher thread synchronization and shutdown signaling. */
    private final Object shutdownGate = new Object();

    /** Trace instance for diagnostic output to DSTrace and trace files. */
    Trace tracer = new Trace("EventLogger");

    /** Database authentication parameters extracted from the init document. */
    AuthenticationParams authParams = null;

    /** Driver option parameters extracted from the init document. */
    ShimParams params = null;

    /** Whether to store the original XDS XML alongside JSON in the database. Controlled by the storeXML driver option. */
    boolean logXML = true;

    /** Target database table name. Controlled by the tableName driver option. */
    String tableName = "public.dxmlevent";

    /** Flag set to {@code true} when {@link #shutdown} is called, signaling the publisher thread to exit. */
    private volatile boolean shutdown = false;

    /** Publisher heartbeat interval in milliseconds, or 0 if disabled. */
    private int heartbeatInterval;

    /** Reusable JDBC connection to PostgreSQL. Validated before each use. */
    private Connection dbConnection = null;

    // Reconnection backoff state
    private static final long BACKOFF_INITIAL_MS = 1000;       // 1 second
    private static final long BACKOFF_MAX_MS = 5 * 60 * 1000;  // 5 minutes
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private long currentBackoffMs = 0;
    private long lastConnectionFailureTime = 0;


    /**
     * Constructs a new EventLoggerDriver instance with default settings.
     */
    public EventLoggerDriver() {
        super("EventLogger");
        this.driverRDN = "";
        this.heartbeatInterval = 0;
    }

    /**
     * Initializes the driver by reading authentication and option parameters from the
     * init document, then validates the database connection.
     * <p>
     * Reads the following from the init document:
     * <ul>
     *   <li>Authentication parameters (user, context/server, password)</li>
     *   <li>Driver options: storeXML, tableName</li>
     *   <li>Publisher heartbeat interval</li>
     * </ul>
     * If the database connection cannot be established, returns {@link #STATUS_FATAL}.
     *
     * @param initParameters the DirXML initialization document
     * @return a status document indicating success or failure
     */
    public XmlDocument init(XmlDocument initParameters) {

        tracer.trace("DirXML Event Logger version " + VERSION, 0);
        authParams = getAuthenticationParams(initParameters.getDocumentNS());

        params = getShimParams(initParameters.getDocumentNS(), "driver", DRIVER_PARAMS);
        setDriverRDN(initParameters.getDocumentNS());

        // Wire up storeXML parameter
        if (params.haveParam("storeXML")) {
            String storeXMLValue = params.getStringParam("storeXML");
            logXML = !"false".equalsIgnoreCase(storeXMLValue);
        }

        // Wire up tableName parameter
        if (params.haveParam("tableName")) {
            String tableNameValue = params.getStringParam("tableName");
            if (tableNameValue != null && !tableNameValue.isEmpty()) {
                tableName = tableNameValue;
            }
        }

        if (this.heartbeatInterval == 0)
        {
            this.heartbeatInterval = getHeartbeatInterval(initParameters.getDocumentNS());
            if (this.heartbeatInterval < 0)
            {
                this.heartbeatInterval = 0;
                return createStatusDocument(STATUS_WARNING, "Ignoring invalid value for <pub-heartbeat-interval>");
            }
        }

        // Validate DB connection on init
        try
        {
            getConnection();
            tracer.trace("Database connection validated successfully", 0);
        } catch (SQLException e)
        {
            tracer.trace("Failed to connect to database during init: " + e.getMessage(), 0);
            return createStatusDocument(STATUS_FATAL, "Database connection failed: " + e.getMessage());
        }

        // Register with PolicyLogger so policies can log events through this driver
        if (driverDN != null && !driverDN.isEmpty()) {
            PolicyLogger.register(driverDN, authParams.authenticationContext,
                    authParams.authenticationId, authParams.applicationPassword,
                    tableName, logXML);
            tracer.trace("Registered PolicyLogger for driver DN: " + driverDN, 0);
        }

        return createSuccessDocument();
    }

    /**
     * Starts the publisher thread. Sends heartbeat status documents at the configured
     * interval, or waits indefinitely if heartbeat is disabled. Runs until
     * {@link #shutdown} is called.
     *
     * @param executePublisher the command processor for submitting documents to the engine
     * @return a success status document when the publisher thread exits
     */
    @Override
    public XmlDocument start(XmlCommandProcessor executePublisher) {
        long nextHeartbeat = System.currentTimeMillis() + this.heartbeatInterval;
        synchronized (this.shutdownGate)
        {
            while (!this.shutdown)
            {
                try
                {
                    if (this.heartbeatInterval != 0)
                    {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime < nextHeartbeat)
                            this.shutdownGate.wait(nextHeartbeat - currentTime);
                        nextHeartbeat = System.currentTimeMillis() + this.heartbeatInterval;
                        Element input = createInputDocument();
                        Element status = addStatusElement(input, 0, null, null);
                        status.setAttributeNS(null, "type", "heartbeat");
                        executePublisher.execute(new XmlDocument(input.getOwnerDocument()), this);
                        continue;
                    }
                    this.shutdownGate.wait();
                } catch (InterruptedException interruptedException)
                {
                }
            }
        }
        return createSuccessDocument();
    }

    /**
     * Shuts down the driver by signaling the publisher thread to exit and
     * closing the database connection.
     *
     * @param reason the shutdown reason document from the engine
     * @return a status document indicating success or failure
     */
    public XmlDocument shutdown(XmlDocument reason) {
        try
        {
            synchronized (this.shutdownGate)
            {
                this.shutdown = true;
                this.shutdownGate.notify();
            }
            // Unregister from PolicyLogger registry before closing connection
            if (driverDN != null && !driverDN.isEmpty()) {
                PolicyLogger.unregister(driverDN);
            }
            closeConnection();
            return createSuccessDocument();
        } catch (Throwable t)
        {
            return createStatusDocument(STATUS_ERROR, t.getMessage());
        }
    }

    /**
     * Returns this driver instance as the subscription shim.
     *
     * @return this instance
     */
    public SubscriptionShim getSubscriptionShim() {

        return this;
    }

    /**
     * Returns this driver instance as the publication shim.
     *
     * @return this instance
     */
    public PublicationShim getPublicationShim() {
        return this;
    }

    /**
     * Returns an empty schema definition. This driver does not define a custom schema
     * as it logs events from all object classes.
     *
     * @param initParameters the schema request document
     * @return an XDS document containing an empty {@code <schema-def>}
     */
    public XmlDocument getSchema(XmlDocument initParameters) {
        setDriverRDN(initParameters.getDocumentNS());
        Element output = createOutputDocument();
        Document doc = output.getOwnerDocument();
        Element schemadef = doc.createElementNS(null, "schema-def");
        output.appendChild(schemadef);
        addStatusElement(output, 0, null, null);
        return new XmlDocument(output.getOwnerDocument());
    }

    /**
     * Handles query requests. Returns success for all queries as this driver
     * does not support application-side queries.
     *
     * @param doc the query document
     * @return a success status document
     */
    @Override
    public XmlDocument query(XmlDocument doc) {
        return createSuccessDocument();
    }

    /**
     * Processes a subscriber channel event by converting it to JSON and writing it
     * to the database.
     * <p>
     * If the document contains a driver identity query, responds with the driver
     * identification instead of logging. Otherwise:
     * <ol>
     *   <li>Determines the event type (add, modify, delete, sync, rename, move)</li>
     *   <li>Converts the XDS XML to JSON via the appropriate {@link BaseEventConverter}</li>
     *   <li>Inserts the event data into PostgreSQL</li>
     * </ol>
     *
     * <h4>Error handling by SQL state:</h4>
     * <ul>
     *   <li><b>23505</b> (duplicate key) — returns ERROR, event already logged</li>
     *   <li><b>42P01/42703</b> (undefined table/column) — returns FATAL</li>
     *   <li><b>23502</b> (not-null violation) — returns ERROR</li>
     *   <li><b>28000</b> (invalid credentials) — returns FATAL</li>
     *   <li><b>08xxx</b> (connection errors) — resets connection, returns RETRY</li>
     *   <li>Other SQL errors — returns RETRY</li>
     * </ul>
     *
     * @param doc   the XDS command document from the subscriber channel
     * @param query the query processor for this execution context
     * @return a status document indicating success, error, fatal, or retry
     */
    public XmlDocument execute(XmlDocument doc, XmlQueryProcessor query) {
        try
        {
            if (isIdentityQuery(doc.getDocument()))
            {
                XmlDocument identXDS = getDriverIdentification("query-driver-ident");
                addStatusElement((Element) identXDS.getDocument().getElementsByTagName("output").item(0), 0, "", "query-driver-ident");
                return identXDS;
            }
            EventRecord record = EventRecords.prepare(doc.getDocumentString());
            tracer.trace("doc (passwords masked): " + record.xml, 3);
            tracer.trace("Converted event to JSON: " + record.json.toString(2), 3);

            tracer.trace("Writing event to database", 3);
            EventRecords.insert(getConnection(), tableName, record, logXML, driverDN);

        } catch (SQLException t)
        {
            tracer.trace("SQL Error processing event: " + t.getMessage() + " SQLState: " + t.getSQLState(), 1);
            String sqlState = t.getSQLState();
            if (sqlState == null) {
                return createStatusDocument(STATUS_RETRY, t.getMessage());
            }

            if (sqlState.equals("23505"))
            {
                return createStatusDocument(STATUS_ERROR, "Duplicate event: " + t.getMessage());
            }
            if (sqlState.equals("42P01") || sqlState.equals("42703"))
            {
                return createStatusDocument(STATUS_FATAL, "Schema error: " + t.getMessage());
            }
            if (sqlState.equals("23502"))
            {
                return createStatusDocument(STATUS_ERROR, "Null constraint violation: " + t.getMessage());
            }
            if (sqlState.equals("28000"))
            {
                tracer.trace("Invalid database credentials", 0);
                return createStatusDocument(STATUS_FATAL, t.getMessage());
            }
            if (sqlState.startsWith("08"))
            {
                closeConnection();
                return createStatusDocument(STATUS_RETRY, "Connection error: " + t.getMessage());
            }
            return createStatusDocument(STATUS_RETRY, t.getMessage());
        } catch (Exception e)
        {
            // Unparseable or unsupported documents will never succeed, so don't retry them
            tracer.trace("Error processing event: " + e.getMessage(), 1);
            return createStatusDocument(STATUS_ERROR, e.getMessage());
        }

        return createStatusDocument(STATUS_SUCCESS, "Event Logged");
    }

    /**
     * Returns true if the document asks for the driver's identity: a {@code <query>}
     * whose {@code <search-class>} is {@code __driver_identification_class__}.
     *
     * @param doc the subscriber command document
     * @return true for a driver identification query
     */
    static boolean isIdentityQuery(Document doc) {
        NodeList classes = doc.getElementsByTagName("search-class");
        for (int i = 0; i < classes.getLength(); i++) {
            Element searchClass = (Element) classes.item(i);
            if ("__driver_identification_class__".equals(searchClass.getAttribute("class-name"))
                    && searchClass.getParentNode() != null
                    && "query".equals(searchClass.getParentNode().getNodeName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds the driver identification response document containing the driver ID,
     * version, minimum activation version, and query-ex-supported flag.
     *
     * @param eventId the event ID for the identification request
     * @return an XDS output document with driver identification attributes
     */
    private XmlDocument getDriverIdentification(String eventId) {
        try
        {
            Element outputElement = createOutputDocument();
            addDriverIdentification(outputElement, this);
            return new XmlDocument(outputElement.getOwnerDocument());
        } catch (Throwable t)
        {
            return createStatusDocument(STATUS_ERROR, t.getMessage());
        }
    }

    /**
     * Gets or creates a database connection, reusing the existing one if still valid.
     * <p>
     * Applies exponential backoff on repeated connection failures to avoid
     * hammering an unavailable database. The backoff sequence is:
     * 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, then capped at 5 minutes.
     * Backoff resets immediately on a successful connection.
     *
     * @return a valid JDBC connection
     * @throws SQLException if the connection cannot be established
     */
    private synchronized Connection getConnection() throws SQLException {
        // Check if existing connection is still usable
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

        // Enforce backoff delay if we've had recent failures
        if (currentBackoffMs > 0)
        {
            long elapsed = System.currentTimeMillis() - lastConnectionFailureTime;
            if (elapsed < currentBackoffMs)
            {
                long waitMs = currentBackoffMs - elapsed;
                tracer.trace("Connection backoff: waiting " + (waitMs / 1000) + "s before retry", 1);
                try
                {
                    Thread.sleep(waitMs);
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted during connection backoff", "08000");
                }
            }
        }

        // Attempt connection
        String url = "jdbc:postgresql://" + authParams.authenticationContext;
        String user = authParams.authenticationId;
        String password = authParams.applicationPassword;
        try
        {
            dbConnection = DriverManager.getConnection(url, user, password);
            if (currentBackoffMs > 0)
            {
                tracer.trace("Database connection restored after backoff", 0);
            }
            currentBackoffMs = 0;
            lastConnectionFailureTime = 0;
            tracer.trace("New database connection established", 2);
            return dbConnection;
        } catch (SQLException e)
        {
            lastConnectionFailureTime = System.currentTimeMillis();
            if (currentBackoffMs == 0)
            {
                currentBackoffMs = BACKOFF_INITIAL_MS;
            } else
            {
                currentBackoffMs = Math.min((long) (currentBackoffMs * BACKOFF_MULTIPLIER), BACKOFF_MAX_MS);
            }
            tracer.trace("Connection failed, next retry backoff: " + (currentBackoffMs / 1000) + "s", 1);
            throw e;
        }
    }

    /**
     * Closes the current database connection if open. Safe to call multiple times.
     */
    private synchronized void closeConnection() {
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
                tracer.trace("Error closing database connection: " + e.getMessage(), 1);
            }
            dbConnection = null;
        }
    }

    /**
     * Populates a driver identification instance element in the output document.
     * Sets the driver ID ("EventLogger"), version, minimum activation version,
     * and query-ex-supported flag.
     *
     * @param output the {@code <output>} element to append the identification to
     * @param query  the query processor (unused, required by interface)
     */
    void addDriverIdentification(Element output, XmlQueryProcessor query) {
        Document doc = output.getOwnerDocument();
        Element instance = doc.createElementNS(null, "instance");
        output.appendChild(instance);
        instance.setAttributeNS(null, "class-name", "__driver_identification_class__");
        Element attr = doc.createElementNS(null, "attr");
        instance.appendChild(attr);
        attr.setAttributeNS(null, "attr-name", "driver-id");
        Element value = doc.createElementNS(null, "value");
        attr.appendChild(value);
        value.setAttributeNS(null, "type", "string");
        Text text = doc.createTextNode("EventLogger");
        value.appendChild(text);
        attr = doc.createElementNS(null, "attr");
        instance.appendChild(attr);
        attr.setAttributeNS(null, "attr-name", "driver-version");
        value = doc.createElementNS(null, "value");
        attr.appendChild(value);
        value.setAttributeNS(null, "type", "string");
        text = doc.createTextNode(VERSION);
        value.appendChild(text);
        attr = doc.createElementNS(null, "attr");
        instance.appendChild(attr);
        attr.setAttributeNS(null, "attr-name", "min-activation-version");
        value = doc.createElementNS(null, "value");
        attr.appendChild(value);
        value.setAttributeNS(null, "type", "int");
        text = doc.createTextNode("0");
        value.appendChild(text);
        attr = doc.createElementNS(null, "attr");
        instance.appendChild(attr);
        attr.setAttributeNS(null, "attr-name", "query-ex-supported");
        value = doc.createElementNS(null, "value");
        attr.appendChild(value);
        value.setAttributeNS(null, "type", "state");
        text = doc.createTextNode("false");
        value.appendChild(text);
    }
}
