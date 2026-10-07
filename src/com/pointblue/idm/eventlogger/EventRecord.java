package com.pointblue.idm.eventlogger;

import com.pointblue.idm.eventlogger.json.JSONObject;

/**
 * One event ready to be stored: the redacted XDS document and the JSON converted from it.
 */
public final class EventRecord {

    /** The event as JSON (from the {@code xds2json} converters, passwords masked). */
    public final JSONObject json;

    /** The XDS document with passwords masked. */
    public final String xml;

    /** For PolicyLogger rows: "subscriber" or "publisher"; null for driver rows. */
    String channel;

    /** For PolicyLogger rows: the policy name or DN as given; null for driver rows. */
    String policy;

    /** For PolicyLogger rows: "input" or "output"; null for driver rows. */
    String stage;

    EventRecord(JSONObject json, String xml) {
        this.json = json;
        this.xml = xml;
    }

    public String eventId() {
        return json.getString("event-id");
    }

    public String eventType() {
        return json.getString("event-type");
    }
}
