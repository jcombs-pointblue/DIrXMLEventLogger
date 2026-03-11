package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converter for transforming XML documents of event type "modify" to JSON format.
 * Based on the nds_2.dtd Document Type Definition.
 */
public class ModifyEventConverter extends BaseEventConverter {

    @Override
    public String convertToJson(String xmlString) throws Exception {
        Document document = parseXmlString(xmlString);

        NodeList modifyNodes = document.getElementsByTagName("modify");
        if (modifyNodes.getLength() == 0) {
            throw new IllegalArgumentException("No 'modify' element found in the XML document");
        }

        Element modifyElement = (Element) modifyNodes.item(0);
        Map<String, Object> jsonMap = convertModifyElementToJson(modifyElement);

        jsonMap.put("event-type", "modify");
        return formatJson(jsonMap, 0);
    }

    private Map<String, Object> convertModifyElementToJson(Element modifyElement) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();

        addAttributes(modifyElement, jsonMap);
        processAssociation(modifyElement, jsonMap);

        // Process modify-attr elements
        NodeList modifyAttrNodes = modifyElement.getElementsByTagName("modify-attr");
        if (modifyAttrNodes.getLength() > 0) {
            Map<String, Object> attributesMap = new LinkedHashMap<>();

            for (int i = 0; i < modifyAttrNodes.getLength(); i++) {
                Element modifyAttrElement = (Element) modifyAttrNodes.item(i);
                String attrName = modifyAttrElement.getAttribute("attr-name");
                Map<String, Object> attrModificationMap = new LinkedHashMap<>();

                // Process add-value elements
                NodeList addValueNodes = modifyAttrElement.getElementsByTagName("add-value");
                if (addValueNodes.getLength() > 0) {
                    List<Object> addValuesList = new ArrayList<>();
                    for (int j = 0; j < addValueNodes.getLength(); j++) {
                        Element valueElement = (Element) addValueNodes.item(j);
                        addValuesList.add(convertValueElementToJson(valueElement));
                    }
                    attrModificationMap.put("add-values", addValuesList);
                }

                // Process remove-value elements
                NodeList removeValueNodes = modifyAttrElement.getElementsByTagName("remove-value");
                if (removeValueNodes.getLength() > 0) {
                    List<Object> removeValuesList = new ArrayList<>();
                    for (int j = 0; j < removeValueNodes.getLength(); j++) {
                        Element valueElement = (Element) removeValueNodes.item(j);
                        removeValuesList.add(convertValueElementToJson(valueElement));
                    }
                    attrModificationMap.put("remove-values", removeValuesList);
                }

                // Process remove-all-values element
                NodeList removeAllValuesNodes = modifyAttrElement.getElementsByTagName("remove-all-values");
                if (removeAllValuesNodes.getLength() > 0) {
                    attrModificationMap.put("remove-all-values", true);
                }

                attributesMap.put(attrName, attrModificationMap);
            }

            jsonMap.put("attributes", attributesMap);
        }

        processPassword(modifyElement, jsonMap);
        processOperationData(modifyElement, jsonMap);

        return jsonMap;
    }

    public static void main(String[] args) {
        String exampleXml = "<nds dtdversion=\"4.0\" ndsversion=\"8.x\">\n" +
                "    <input>\n" +
                "        <modify class-name=\"user\" event-id=\"1\" src-dn=\"\\PERIN-TAO\\novell\\John\" src-entry-id=\"35868\">\n" +
                "            <modify-attr attr-name=\"firstName\">\n" +
                "                <remove-value>\n" +
                "                    <value timestamp=\"965252229#1\" type=\"string\">Frank</value>\n" +
                "                </remove-value>\n" +
                "                <add-value>\n" +
                "                    <value timestamp=\"965252229#2\" type=\"string\">John</value>\n" +
                "                </add-value>\n" +
                "            </modify-attr>\n" +
                "        </modify>\n" +
                "    </input>\n" +
                "</nds>";

        try {
            ModifyEventConverter converter = new ModifyEventConverter();
            String jsonResult = converter.convertToJson(exampleXml);
            System.out.println("Converted JSON:");
            System.out.println(jsonResult);
        } catch (Exception e) {
            System.err.println("Error converting XML to JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }
}