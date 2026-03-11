package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for XDS-to-JSON event converters.
 * Contains shared XML parsing, JSON formatting, and utility methods.
 */
public abstract class BaseEventConverter {

    /**
     * Converts an XML string to a JSON string.
     *
     * @param xmlString The XML string to convert
     * @return A JSON string representation of the XML
     * @throws Exception If there is an error during conversion
     */
    public abstract String convertToJson(String xmlString) throws Exception;

    /**
     * Parses an XML string into a Document object with XXE protection.
     *
     * @param xmlString The XML string to parse
     * @return The parsed Document
     * @throws ParserConfigurationException If there is a configuration error
     * @throws SAXException If there is an error during parsing
     * @throws IOException If there is an I/O error
     */
    protected Document parseXmlString(String xmlString)
            throws ParserConfigurationException, SAXException, IOException {
        if (xmlString == null || xmlString.isEmpty()) {
            throw new IllegalArgumentException("XML string must not be null or empty");
        }
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false);
        factory.setNamespaceAware(false);
        // XXE protection
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);

        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(xmlString)));
    }

    /**
     * Adds all attributes of an element to a Map.
     *
     * @param element The element whose attributes to add
     * @param map The Map to add the attributes to
     */
    protected void addAttributes(Element element, Map<String, Object> map) {
        for (int i = 0; i < element.getAttributes().getLength(); i++) {
            Node attribute = element.getAttributes().item(i);
            map.put(attribute.getNodeName(), attribute.getNodeValue());
        }
    }

    /**
     * Processes an association element and adds it to the JSON map.
     *
     * @param parentElement The parent element containing the association
     * @param jsonMap The map to add the association to
     */
    protected void processAssociation(Element parentElement, Map<String, Object> jsonMap) {
        NodeList associationNodes = parentElement.getElementsByTagName("association");
        if (associationNodes.getLength() > 0) {
            Element associationElement = (Element) associationNodes.item(0);
            Map<String, Object> associationMap = new LinkedHashMap<>();

            addAttributes(associationElement, associationMap);

            String textContent = associationElement.getTextContent();
            if (textContent != null) {
                textContent = textContent.trim();
                if (!textContent.isEmpty()) {
                    associationMap.put("value", textContent);
                }
            }

            jsonMap.put("association", associationMap);
        }
    }

    /**
     * Processes operation-data element and adds it to the JSON map.
     *
     * @param parentElement The parent element containing the operation-data
     * @param jsonMap The map to add the operation data to
     */
    protected void processOperationData(Element parentElement, Map<String, Object> jsonMap) {
        NodeList operationDataNodes = parentElement.getElementsByTagName("operation-data");
        if (operationDataNodes.getLength() > 0) {
            Element operationDataElement = (Element) operationDataNodes.item(0);
            Map<String, Object> operationDataMap = new LinkedHashMap<>();

            NodeList childNodes = operationDataElement.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node childNode = childNodes.item(i);
                if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element childElement = (Element) childNode;
                    String text = childElement.getTextContent();
                    operationDataMap.put(childElement.getNodeName(), text != null ? text.trim() : "");
                }
            }

            jsonMap.put("operationData", operationDataMap);
        }
    }

    /**
     * Processes a password element and adds it to the JSON map.
     *
     * @param parentElement The parent element containing the password
     * @param jsonMap The map to add the password to
     */
    protected void processPassword(Element parentElement, Map<String, Object> jsonMap) {
        NodeList passwordNodes = parentElement.getElementsByTagName("password");
        if (passwordNodes.getLength() > 0) {
            Element passwordElement = (Element) passwordNodes.item(0);
            String password = passwordElement.getTextContent();
            jsonMap.put("password", password != null ? password.trim() : "");
        }
    }

    /**
     * Converts a value element to a JSON object or string.
     *
     * @param valueElement The value element to convert
     * @return A Map or String representing the value element
     */
    protected Object convertValueElementToJson(Element valueElement) {
        NodeList componentNodes = valueElement.getElementsByTagName("component");

        if (componentNodes.getLength() > 0) {
            Map<String, Object> valueMap = new LinkedHashMap<>();
            addAttributes(valueElement, valueMap);

            Map<String, Object> componentsMap = new LinkedHashMap<>();
            for (int i = 0; i < componentNodes.getLength(); i++) {
                Element componentElement = (Element) componentNodes.item(i);
                String componentName = componentElement.getAttribute("name");
                String componentValue = componentElement.getTextContent();
                componentsMap.put(componentName, componentValue != null ? componentValue.trim() : "");
            }

            valueMap.put("components", componentsMap);
            return valueMap;
        } else {
            String textContent = valueElement.getTextContent();
            textContent = textContent != null ? textContent.trim() : "";

            if (valueElement.getAttributes().getLength() > 0) {
                Map<String, Object> valueMap = new LinkedHashMap<>();
                addAttributes(valueElement, valueMap);
                valueMap.put("value", textContent);
                return valueMap;
            } else {
                return textContent;
            }
        }
    }

    /**
     * Processes add-attr elements (used by add and sync events).
     *
     * @param parentElement The parent element containing add-attr children
     * @param jsonMap The map to add the attributes to
     */
    protected void processAddAttrs(Element parentElement, Map<String, Object> jsonMap) {
        NodeList addAttrNodes = parentElement.getElementsByTagName("add-attr");
        if (addAttrNodes.getLength() > 0) {
            Map<String, Object> attributesMap = new LinkedHashMap<>();

            for (int i = 0; i < addAttrNodes.getLength(); i++) {
                Element addAttrElement = (Element) addAttrNodes.item(i);
                String attrName = addAttrElement.getAttribute("attr-name");

                NodeList valueNodes = addAttrElement.getElementsByTagName("value");
                if (valueNodes.getLength() == 1) {
                    Element valueElement = (Element) valueNodes.item(0);
                    attributesMap.put(attrName, convertValueElementToJson(valueElement));
                } else if (valueNodes.getLength() > 1) {
                    List<Object> valuesList = new ArrayList<>();
                    for (int j = 0; j < valueNodes.getLength(); j++) {
                        Element valueElement = (Element) valueNodes.item(j);
                        valuesList.add(convertValueElementToJson(valueElement));
                    }
                    attributesMap.put(attrName, valuesList);
                }
            }

            jsonMap.put("attributes", attributesMap);
        }
    }

    /**
     * Formats a Map or List as a JSON string with indentation.
     */
    protected String formatJson(Object obj, int indent) {
        if (obj == null) {
            return "null";
        }

        if (obj instanceof Map) {
            return formatMap((Map<String, Object>) obj, indent);
        } else if (obj instanceof List) {
            return formatList((List<Object>) obj, indent);
        } else if (obj instanceof String) {
            return "\"" + escapeJsonString((String) obj) + "\"";
        } else {
            return obj.toString();
        }
    }

    private String formatMap(Map<String, Object> map, int indent) {
        if (map.isEmpty()) {
            return "{}";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");

        String indentStr = repeatString(" ", (indent + 1) * 2);
        boolean first = true;

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                sb.append(",\n");
            }
            first = false;

            sb.append(indentStr)
              .append("\"")
              .append(escapeJsonString(entry.getKey()))
              .append("\": ")
              .append(formatJson(entry.getValue(), indent + 1));
        }

        sb.append("\n").append(repeatString(" ", indent * 2)).append("}");
        return sb.toString();
    }

    private String formatList(List<Object> list, int indent) {
        if (list.isEmpty()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("[\n");

        String indentStr = repeatString(" ", (indent + 1) * 2);
        boolean first = true;

        for (Object item : list) {
            if (!first) {
                sb.append(",\n");
            }
            first = false;

            sb.append(indentStr)
              .append(formatJson(item, indent + 1));
        }

        sb.append("\n").append(repeatString(" ", indent * 2)).append("]");
        return sb.toString();
    }

    private String escapeJsonString(String str) {
        return str.replace("\\", "\\\\")
                 .replace("\"", "\\\"")
                 .replace("\b", "\\b")
                 .replace("\f", "\\f")
                 .replace("\n", "\\n")
                 .replace("\r", "\\r")
                 .replace("\t", "\\t");
    }

    private String repeatString(String str, int count) {
        if (count <= 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}