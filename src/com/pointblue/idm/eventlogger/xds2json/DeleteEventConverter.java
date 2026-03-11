package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converter for transforming XML documents of event type "delete" to JSON format.
 * Based on the nds_2.dtd Document Type Definition.
 */
public class DeleteEventConverter extends BaseEventConverter {

    @Override
    public String convertToJson(String xmlString) throws Exception {
        Document document = parseXmlString(xmlString);

        NodeList deleteNodes = document.getElementsByTagName("delete");
        if (deleteNodes.getLength() == 0) {
            throw new IllegalArgumentException("No 'delete' element found in the XML document");
        }

        Element deleteElement = (Element) deleteNodes.item(0);
        Map<String, Object> jsonMap = convertDeleteElementToJson(deleteElement);

        jsonMap.put("event-type", "delete");
        return formatJson(jsonMap, 0);
    }

    private Map<String, Object> convertDeleteElementToJson(Element deleteElement) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        addAttributes(deleteElement, jsonMap);
        processAssociation(deleteElement, jsonMap);
        processOperationData(deleteElement, jsonMap);

        return jsonMap;
    }

    public static void main(String[] args) {
        String exampleXml = "<nds dtdversion=\"4.0\" ndsversion=\"8.x\">\n" +
                "    <input>\n" +
                "        <delete class-name=\"user\" event-id=\"2\" src-dn=\"\\PERIN-TAO\\novell\\John\" src-entry-id=\"35868\">\n" +
                "            <association>CN=jdoe,OU=users,O=company</association>\n" +
                "        </delete>\n" +
                "    </input>\n" +
                "</nds>";

        try {
            DeleteEventConverter converter = new DeleteEventConverter();
            String jsonResult = converter.convertToJson(exampleXml);
            System.out.println("Converted JSON:");
            System.out.println(jsonResult);
        } catch (Exception e) {
            System.err.println("Error converting XML to JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }
}