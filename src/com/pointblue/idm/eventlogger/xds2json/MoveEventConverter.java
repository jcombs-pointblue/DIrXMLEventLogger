package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converter for transforming XML documents of event type "move" to JSON format.
 * Based on the nds_2.dtd Document Type Definition.
 */
public class MoveEventConverter extends BaseEventConverter {

    @Override
    public String convertToJson(String xmlString) throws Exception {
        Document document = parseXmlString(xmlString);

        NodeList moveNodes = document.getElementsByTagName("move");
        if (moveNodes.getLength() == 0) {
            throw new IllegalArgumentException("No 'move' element found in the XML document");
        }

        Element moveElement = (Element) moveNodes.item(0);
        Map<String, Object> jsonMap = convertMoveElementToJson(moveElement);

        jsonMap.put("event-type", "move");
        return formatJson(jsonMap, 0);
    }

    private Map<String, Object> convertMoveElementToJson(Element moveElement) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        addAttributes(moveElement, jsonMap);
        processAssociation(moveElement, jsonMap);

        // Process parent element (destination container)
        NodeList parentNodes = moveElement.getElementsByTagName("parent");
        if (parentNodes.getLength() > 0) {
            Element parentElement = (Element) parentNodes.item(0);
            Map<String, Object> parentMap = new LinkedHashMap<>();
            addAttributes(parentElement, parentMap);
            String textContent = parentElement.getTextContent();
            if (textContent != null && !textContent.trim().isEmpty()) {
                parentMap.put("value", textContent.trim());
            }
            jsonMap.put("parent", parentMap);
        }

        processOperationData(moveElement, jsonMap);

        return jsonMap;
    }

    public static void main(String[] args) {
        String exampleXml = "<nds dtdversion=\"4.0\" ndsversion=\"8.x\">\n" +
                "    <input>\n" +
                "        <move class-name=\"user\" event-id=\"5\" src-dn=\"\\PERIN-TAO\\novell\\John\" src-entry-id=\"35868\">\n" +
                "            <association>CN=jdoe,OU=users,O=company</association>\n" +
                "            <parent src-dn=\"\\PERIN-TAO\\novell\\archive\"/>\n" +
                "        </move>\n" +
                "    </input>\n" +
                "</nds>";

        try {
            MoveEventConverter converter = new MoveEventConverter();
            String jsonResult = converter.convertToJson(exampleXml);
            System.out.println("Converted JSON:");
            System.out.println(jsonResult);
        } catch (Exception e) {
            System.err.println("Error converting XML to JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }
}