package com.pointblue.idm.eventlogger;

import com.pointblue.idm.eventlogger.json.JSONObject;
import com.pointblue.idm.eventlogger.xds2json.*;
import org.postgresql.util.PGobject;
import org.w3c.dom.Document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

/**
 * Turns an XDS document into an {@link EventRecord} and writes it to the event table.
 * Shared by {@link EventLoggerDriver} and {@link PolicyLogger}; has no engine dependencies.
 */
public final class EventRecords {

    /** Event types in detection order. */
    private static final String[] EVENT_TYPES = {"add", "modify", "delete", "sync", "rename", "move"};

    private EventRecords() {
    }

    /**
     * Redacts passwords, detects the event type and converts the document to JSON.
     *
     * @param xml the XDS document
     * @return the prepared record
     * @throws IllegalArgumentException if the document holds no supported event
     * @throws Exception                if the XML cannot be parsed or converted
     */
    public static EventRecord prepare(String xml) throws Exception {
        String redacted = PasswordRedactor.redact(xml);
        String type = detectType(XmlSupport.parse(redacted));
        if (type == null) {
            throw new IllegalArgumentException("Unsupported event type. Supported types: add, modify, delete, sync, rename, move.");
        }
        JSONObject json = new JSONObject(converterFor(type).convertToJson(redacted));
        return new EventRecord(json, redacted);
    }

    /** Returns the first supported event type found in the document, or null. */
    /**
     * Marks a record as logged by a policy, normalizing channel and stage.
     *
     * @param record  the record to mark
     * @param channel "subscriber" or "publisher" ("sub" and "pub" are accepted)
     * @param policy  the policy name or DN, stored as given
     * @param stage   "input" or "output"; null or empty means "input"
     * @throws IllegalArgumentException if channel or stage is not one of the accepted values
     */
    public static void setPolicySource(EventRecord record, String channel, String policy, String stage) {
        record.channel = normalizeChannel(channel);
        record.policy = policy;
        record.stage = normalizeStage(stage);
    }

    static String normalizeChannel(String channel) {
        String c = channel == null ? "" : channel.trim().toLowerCase(java.util.Locale.ROOT);
        if (c.equals("sub") || c.equals("subscriber")) {
            return "subscriber";
        }
        if (c.equals("pub") || c.equals("publisher")) {
            return "publisher";
        }
        throw new IllegalArgumentException("channel must be subscriber or publisher, was: " + channel);
    }

    static String normalizeStage(String stage) {
        String s = stage == null ? "" : stage.trim().toLowerCase(java.util.Locale.ROOT);
        if (s.isEmpty() || s.equals("input")) {
            return "input";
        }
        if (s.equals("output")) {
            return "output";
        }
        throw new IllegalArgumentException("stage must be input or output, was: " + stage);
    }

    static String detectType(Document doc) {
        for (String type : EVENT_TYPES) {
            if (doc.getElementsByTagName(type).getLength() > 0) {
                return type;
            }
        }
        return null;
    }

    static BaseEventConverter converterFor(String eventType) {
        switch (eventType) {
            case "add":    return new AddEventConverter();
            case "modify": return new ModifyEventConverter();
            case "delete": return new DeleteEventConverter();
            case "sync":   return new SyncEventConverter();
            case "rename": return new RenameEventConverter();
            case "move":   return new MoveEventConverter();
            default: throw new IllegalArgumentException("Unknown event type: " + eventType);
        }
    }

    /**
     * Inserts a record into the event table.
     *
     * @param conn      the connection to use
     * @param tableName the target table
     * @param record    the record to store
     * @param storeXML  {@code true} to store the (redacted) XML, {@code false} to store NULL
     * @param srcDriver the DN of the driver the event came from, or null
     * @throws SQLException if the insert fails
     */
    public static void insert(Connection conn, String tableName, EventRecord record,
                              boolean storeXML, String srcDriver) throws SQLException {
        String sql = "INSERT INTO " + tableName + " (\"eventid\", \"classname\", \"srcdn\", \"srcentryid\", \"eventtype\", \"eventjson\", \"cachedtime\", \"xmlevent\", \"srcdriver\", \"channel\", \"policy\", \"stage\") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
        JSONObject json = record.json;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            long epochSeconds = Long.parseLong(json.getString("timestamp").split("#")[0]);
            pstmt.setString(1, json.getString("event-id"));
            pstmt.setString(2, json.getString("class-name"));
            pstmt.setString(3, json.getString("src-dn"));
            pstmt.setString(4, json.getString("src-entry-id"));
            pstmt.setString(5, json.getString("event-type"));

            PGobject jsonObject = new PGobject();
            jsonObject.setType("json");
            jsonObject.setValue(json.toString());
            pstmt.setObject(6, jsonObject);

            pstmt.setTimestamp(7, Timestamp.from(Instant.ofEpochSecond(epochSeconds)));
            setNullable(pstmt, 8, storeXML ? record.xml : null);
            setNullable(pstmt, 9, srcDriver);
            setNullable(pstmt, 10, record.channel);
            setNullable(pstmt, 11, record.policy);
            setNullable(pstmt, 12, record.stage);
            pstmt.executeUpdate();
        }
    }

    private static void setNullable(PreparedStatement pstmt, int index, String value) throws SQLException {
        if (value != null) {
            pstmt.setString(index, value);
        } else {
            pstmt.setNull(index, Types.VARCHAR);
        }
    }
}
