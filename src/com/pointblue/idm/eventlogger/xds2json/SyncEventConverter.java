package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converter for transforming XML documents of event type "sync" to JSON format.
 * Based on the nds_2.dtd Document Type Definition.
 */
public class SyncEventConverter extends BaseEventConverter {

    @Override
    public String convertToJson(String xmlString) throws Exception {
        Document document = parseXmlString(xmlString);

        NodeList syncNodes = document.getElementsByTagName("sync");
        if (syncNodes.getLength() == 0) {
            throw new IllegalArgumentException("No 'sync' element found in the XML document");
        }

        Element syncElement = (Element) syncNodes.item(0);
        Map<String, Object> jsonMap = convertSyncElementToJson(syncElement);

        jsonMap.put("event-type", "sync");
        return formatJson(jsonMap, 0);
    }

    private Map<String, Object> convertSyncElementToJson(Element syncElement) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        addAttributes(syncElement, jsonMap);
        processAssociation(syncElement, jsonMap);
        processAddAttrs(syncElement, jsonMap);

        // Process status element
        NodeList statusNodes = syncElement.getElementsByTagName("status");
        if (statusNodes.getLength() > 0) {
            Element statusElement = (Element) statusNodes.item(0);
            String statusValue = statusElement.getTextContent();
            jsonMap.put("status", statusValue != null ? statusValue.trim() : "");
        }

        processOperationData(syncElement, jsonMap);

        return jsonMap;
    }

    public static void main(String[] args) {
        String exampleXml = "<nds dtdversion=\"4.0\" ndsversion=\"8.x\">\n" +
                "    <input>\n" +
                "        <sync class-name=\"user\" event-id=\"4\" src-dn=\"\\PERIN-TAO\\novell\\JohnDoe\" src-entry-id=\"35868\">\n" +
                "            <association>CN=jdoe,OU=users,O=company</association>\n" +
                "            <add-attr attr-name=\"username\">\n" +
                "                <value timestamp=\"965252229#1\" type=\"string\">jdoe</value>\n" +
                "            </add-attr>\n" +
                "            <status>success</status>\n" +
                "        </sync>\n" +
                "    </input>\n" +
                "</nds>";

        try {
            SyncEventConverter converter = new SyncEventConverter();
            String jsonResult = converter.convertToJson(exampleXml);
            System.out.println("Converted JSON:");
            System.out.println(jsonResult);
        } catch (Exception e) {
            System.err.println("Error converting XML to JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }
}