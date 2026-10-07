package com.pointblue.idm.eventlogger;

import com.pointblue.idm.eventlogger.json.JSONObject;
import com.pointblue.idm.eventlogger.xds2json.BaseEventConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaVersionTest {

    @ParameterizedTest
    @ValueSource(strings = {"add", "modify", "delete", "sync", "rename", "move"})
    void everyConverterWritesSchemaVersion(String type) throws Exception {
        String xml = "<nds><input><" + type + " class-name=\"User\" src-dn=\"\\T\\u\" event-id=\"1#1\"/></input></nds>";
        JSONObject json = new JSONObject(EventRecords.converterFor(type).convertToJson(xml));
        assertEquals(BaseEventConverter.SCHEMA_VERSION, json.getInt("schemaVersion"));
        assertEquals(2, json.getInt("schemaVersion"));
        assertEquals(type, json.getString("event-type"));
    }
}
