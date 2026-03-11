package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converter for transforming XML documents of event type "rename" to JSON format.
 * Based on the nds_2.dtd Document Type Definition.
 */
public class RenameEventConverter extends BaseEventConverter {

    @Override
    public String convertToJson(String xmlString) throws Exception {
        Document document = parseXmlString(xmlString);

        NodeList renameNodes = document.getElementsByTagName("rename");
        if (renameNodes.getLength() == 0) {
            throw new IllegalArgumentException("No 'rename' element found in the XML document");
        }

        Element renameElement = (Element) renameNodes.item(0);
        Map<String, Object> jsonMap = convertRenameElementToJson(renameElement);

        jsonMap.put("event-type", "rename");
        return formatJson(jsonMap, 0);
    }

    private Map<String, Object> convertRenameElementToJson(Element renameElement) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        addAttributes(renameElement, jsonMap);
        processAssociation(renameElement, jsonMap);

        // Process from element
        NodeList fromNodes = renameElement.getElementsByTagName("from");
        if (fromNodes.getLength() > 0) {
            Element fromElement = (Element) fromNodes.item(0);
            String fromValue = fromElement.getTextContent();
            jsonMap.put("from", fromValue != null ? fromValue.trim() : "");
        }

        // Process to element
        NodeList toNodes = renameElement.getElementsByTagName("to");
        if (toNodes.getLength() > 0) {
            Element toElement = (Element) toNodes.item(0);
            String toValue = toElement.getTextContent();
            jsonMap.put("to", toValue != null ? toValue.trim() : "");
        }

        processOperationData(renameElement, jsonMap);

        return jsonMap;
    }

    public static void main(String[] args) {
        String exampleXml = "<nds dtdversion=\"4.0\" ndsversion=\"8.x\">\n" +
                "    <input>\n" +
                "        <rename class-name=\"user\" event-id=\"3\" src-dn=\"\\PERIN-TAO\\novell\\John\" src-entry-id=\"35868\">\n" +
                "            <association>CN=jdoe,OU=users,O=company</association>\n" +
                "            <from>\\PERIN-TAO\\novell\\John</from>\n" +
                "            <to>\\PERIN-TAO\\novell\\JohnDoe</to>\n" +
                "        </rename>\n" +
                "    </input>\n" +
                "</nds>";

        try {
            RenameEventConverter converter = new RenameEventConverter();
            String jsonResult = converter.convertToJson(exampleXml);
            System.out.println("Converted JSON:");
            System.out.println(jsonResult);
        } catch (Exception e) {
            System.err.println("Error converting XML to JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }
}