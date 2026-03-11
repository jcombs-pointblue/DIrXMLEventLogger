package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converter for transforming XML documents of event type "add" to JSON format.
 * Based on the nds_2.dtd Document Type Definition.
 */
public class AddEventConverter extends BaseEventConverter {

    @Override
    public String convertToJson(String xmlString) throws Exception {
        Document document = parseXmlString(xmlString);

        NodeList addNodes = document.getElementsByTagName("add");
        if (addNodes.getLength() == 0) {
            throw new IllegalArgumentException("No 'add' element found in the XML document");
        }

        Element addElement = (Element) addNodes.item(0);
        Map<String, Object> jsonMap = convertAddElementToJson(addElement);

        jsonMap.put("event-type", "add");
        return formatJson(jsonMap, 0);
    }

    private Map<String, Object> convertAddElementToJson(Element addElement) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        addAttributes(addElement, jsonMap);
        processAssociation(addElement, jsonMap);
        processAddAttrs(addElement, jsonMap);
        processPassword(addElement, jsonMap);
        processOperationData(addElement, jsonMap);

        return jsonMap;
    }

    public static void main(String[] args) {
        String exampleXml = "<nds dtdversion=\"4.0\" ndsversion=\"8.x\">\n" +
                "    <input>\n" +
                "        <add class-name=\"user\" event-id=\"0\" src-dn=\"\\PERIN-TAO\\novell\\John\" src-entry-id=\"35868\">\n" +
                "            <add-attr attr-name=\"username\">\n" +
                "                <value timestamp=\"965252229#1\" type=\"string\">Z123483</value>\n" +
                "            </add-attr>\n" +
                "            <add-attr attr-name=\"lastName\">\n" +
                "                <value timestamp=\"965252204#5\" type=\"string\">Doe</value>\n" +
                "            </add-attr>\n" +
                "        </add>\n" +
                "    </input>\n" +
                "</nds>";

        try {
            AddEventConverter converter = new AddEventConverter();
            String jsonResult = converter.convertToJson(exampleXml);
            System.out.println("Converted JSON:");
            System.out.println(jsonResult);
        } catch (Exception e) {
            System.err.println("Error converting XML to JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }
}