package com.pointblue.idm.eventlogger;

import com.novell.nds.dirxml.driver.*;
import com.novell.nds.dirxml.driver.xds.XDSCommandDocument;
import com.novell.nds.dirxml.driver.xds.XDSParseException;
import com.pointblue.idm.eventlogger.json.JSONObject;
import com.pointblue.idm.eventlogger.xds2json.*;
import org.postgresql.util.PGobject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import java.sql.*;
import java.time.Instant;


/**
 * EventLoggerDriver is the main class for the DirXML Event Logger driver.
 * This driver logs events that pass through the Identity Manager system
 * to a PostgreSQL database.
 */
public class EventLoggerDriver extends CommonImpl implements DriverShim, PublicationShim, SubscriptionShim, XmlQueryProcessor {

    /**
     * Table of parameters that this driver shim wants to get from the
     * &lt;driver-options> element of the init-params.
     */
    public static final ShimParamDesc[] DRIVER_PARAMS
            = {
            new ShimParamDesc("storeXML", ShimParamDesc.STRING_TYPE, false),
            new ShimParamDesc("tableName", ShimParamDesc.STRING_TYPE, false)

    };
    private static final String DRIVER_VERSION_VALUE = "1.0.0";
    private static final String DRIVER_MIN_ACTIVATION_VERSION = "0";

    private final Object shutdownGate = new Object();
    Trace tracer = new Trace("EventLogger");
    AuthenticationParams authParams = null;
    ShimParams params = null;
    boolean logXML = true;
    String tableName = "public.dxmlevent";
    private volatile boolean shutdown = false;
    private int heartbeatInterval;
    private Connection dbConnection = null;

    // Reconnection backoff state
    private static final long BACKOFF_INITIAL_MS = 1000;       // 1 second
    private static final long BACKOFF_MAX_MS = 5 * 60 * 1000;  // 5 minutes
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private long currentBackoffMs = 0;
    private long lastConnectionFailureTime = 0;


    public EventLoggerDriver() {
        super("EventLogger");
        this.driverRDN = "";
        this.heartbeatInterval = 0;
    }

    /**
     * Initialize the driver with the given document.
     *
     * @param initParameters The initialization document
     * @return The result of initialization
     */
    public XmlDocument init(XmlDocument initParameters) {

        authParams = getAuthenticationParams(initParameters.getDocumentNS());

        //get any non-authentication options from the init document
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

        return createSuccessDocument();
    }

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
     * Shut down the driver.
     *
     * @param reason The shutdown document
     * @return The result of shutdown
     */
    public XmlDocument shutdown(XmlDocument reason) {
        try
        {
            synchronized (this.shutdownGate)
            {
                this.shutdown = true;
                this.shutdownGate.notify();
            }
            closeConnection();
            return createSuccessDocument();
        } catch (Throwable t)
        {
            return createStatusDocument(STATUS_ERROR, t.getMessage());
        }
    }

    /**
     * Get the subscription shim for this driver.
     *
     * @return The subscription shim
     */
    public SubscriptionShim getSubscriptionShim() {

        return this;
    }

    /**
     * Get the publication shim for this driver.
     *
     * @return The publication shim
     */
    public PublicationShim getPublicationShim() {
        return this;
    }

    /**
     * Get the schema for this driver.
     *
     * @param initParameters The document requesting schema information
     * @return The schema document
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

    @Override
    public XmlDocument query(XmlDocument doc) {
        return createSuccessDocument();
    }

    public XmlDocument execute(XmlDocument doc, XmlQueryProcessor query) {
        try
        {
            tracer.trace("doc: " + new XmlDocument(doc.getDocumentNS()).getDocumentString(), 3);
            XDSCommandDocument commands = new XDSCommandDocument(doc);
            if (commands.containsIdentityQuery())
            {
                XmlDocument identXDS = getDriverIdentification("query-driver-ident");
                addStatusElement((Element) identXDS.getDocument().getElementsByTagName("output").item(0), 0, "", "query-driver-ident");
                return identXDS;
            }
            // Process the event and log it
            JSONObject eventJSON = convertEvent(doc);

            tracer.trace("Converted event to JSON: " + eventJSON.toString(2), 3);

            writeEventToDB(eventJSON, doc);

        } catch (SQLException t)
        {
            tracer.trace("SQL Error processing event: " + t.getMessage() + " SQLState: " + t.getSQLState(), 1);
            String sqlState = t.getSQLState();
            if (sqlState == null) {
                return createStatusDocument(STATUS_RETRY, t.getMessage());
            }

            // Duplicate key — event already logged
            if (sqlState.equals("23505"))
            {
                return createStatusDocument(STATUS_ERROR, "Duplicate event: " + t.getMessage());
            }
            // Schema errors — won't self-resolve, require admin intervention
            if (sqlState.equals("42P01") || sqlState.equals("42703"))
            {
                return createStatusDocument(STATUS_FATAL, "Schema error: " + t.getMessage());
            }
            // Not-null constraint violation
            if (sqlState.equals("23502"))
            {
                return createStatusDocument(STATUS_ERROR, "Null constraint violation: " + t.getMessage());
            }
            // Invalid credentials
            if (sqlState.equals("28000"))
            {
                tracer.trace("Invalid database credentials", 0);
                return createStatusDocument(STATUS_FATAL, t.getMessage());
            }
            // Connection errors (08xxx) — transient, retry
            if (sqlState.startsWith("08"))
            {
                closeConnection();
                return createStatusDocument(STATUS_RETRY, "Connection error: " + t.getMessage());
            }
            return createStatusDocument(STATUS_RETRY, t.getMessage());
        } catch (XDSParseException e)
        {
            tracer.trace("Error processing event: " + e.getMessage(), 1);
            return createStatusDocument(STATUS_RETRY, e.getMessage());
        } catch (Exception e)
        {
            tracer.trace("Error processing event: " + e.getMessage(), 1);
            return createStatusDocument(STATUS_ERROR, e.getMessage());
        }

        return createStatusDocument(STATUS_SUCCESS, "Event Logged");
    }

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
     * Gets or creates a database connection. Reuses existing connection if still valid.
     * Applies exponential backoff (1s, 2s, 4s, ... up to 5min) on repeated connection failures
     * to avoid hammering an unavailable database. Backoff resets on successful connection.
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
            // Success — reset backoff
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
            // Advance backoff for next attempt
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
     * Closes the current database connection if open.
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

    private void writeEventToDB(JSONObject eventJSON, XmlDocument doc) throws SQLException {
        tracer.trace("Writing event to database", 3);

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
        text = doc.createTextNode(DRIVER_VERSION_VALUE);
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

    /**
     * Converts an XML event document to a JSON object.
     * Determines the event type (add, modify, delete, sync, rename, move) and uses the appropriate converter.
     *
     * @param xmlDoc The XML document to convert
     * @return A JSON object representing the event
     * @throws Exception If there is an error during conversion
     */
    private JSONObject convertEvent(XmlDocument xmlDoc) throws Exception {
        Document doc = xmlDoc.getDocument();
        String xmlString = xmlDoc.getDocumentString();

        String[][] eventTypes = {
            {"add", "add"},
            {"modify", "modify"},
            {"delete", "delete"},
            {"sync", "sync"},
            {"rename", "rename"},
            {"move", "move"}
        };

        for (String[] eventType : eventTypes)
        {
            NodeList nodes = doc.getElementsByTagName(eventType[0]);
            if (nodes.getLength() > 0)
            {
                tracer.trace("Processing " + eventType[1] + " event", 3);
                BaseEventConverter converter = getConverterForType(eventType[1]);
                String jsonString = converter.convertToJson(xmlString);
                return new JSONObject(jsonString);
            }
        }

        throw new IllegalArgumentException("Unsupported event type. Supported types: add, modify, delete, sync, rename, move.");
    }

    private BaseEventConverter getConverterForType(String eventType) {
        switch (eventType)
        {
            case "add":
                return new AddEventConverter();
            case "modify":
                return new ModifyEventConverter();
            case "delete":
                return new DeleteEventConverter();
            case "sync":
                return new SyncEventConverter();
            case "rename":
                return new RenameEventConverter();
            case "move":
                return new MoveEventConverter();
            default:
                throw new IllegalArgumentException("Unknown event type: " + eventType);
        }
    }
}