package com.pointblue.idm.eventlogger.xds2json;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converter for transforming JSON back to XML documents of event type "add".
 * This class performs the reverse operation of AddEventConverter.
 */
public class JsonToXmlConverter {

    /**
     * Constructor for the JSON to XML converter.
     */
    public JsonToXmlConverter() {
        // No initialization needed
    }

    /**
     * Converts a JSON string back to an XML string.
     *
     * @param jsonString The JSON string to convert
     * @return An XML string representation of the JSON
     * @throws Exception If there is an error during conversion
     */
    public String convertToXml(String jsonString) throws Exception {
        // Parse the JSON string to a Map
        Map<String, Object> jsonMap = parseJsonString(jsonString);

        // Create a new XML document
        Document document = createDocument();

        // Create the root elements
        Element ndsElement = document.createElement("nds");
        ndsElement.setAttribute("dtdversion", "4.0");
        ndsElement.setAttribute("ndsversion", "8.x");
        ndsElement.setAttribute("xmlns:cmd", "http://www.novell.com/nxsl/java/com.novell.nds.dirxml.driver.XdsCommandProcessor");
        ndsElement.setAttribute("xmlns:jstring", "http://www.novell.com/nxsl/java/java.lang.String");
        ndsElement.setAttribute("xmlns:query", "http://www.novell.com/nxsl/java/com.novell.nds.dirxml.driver.XdsQueryProcessor");
        document.appendChild(ndsElement);

        // Create source element
        Element sourceElement = document.createElement("source");
        ndsElement.appendChild(sourceElement);

        Element productElement = document.createElement("product");
        productElement.setAttribute("version", "1.0");
        productElement.setTextContent("DirXML");
        sourceElement.appendChild(productElement);

        Element contactElement = document.createElement("contact");
        contactElement.setTextContent("Novell, Inc.");
        sourceElement.appendChild(contactElement);

        // Create input element
        Element inputElement = document.createElement("input");
        ndsElement.appendChild(inputElement);

        // Determine the event type and create the appropriate element
        Element eventElement;
        if (isDeleteEvent(jsonMap)) {
            eventElement = createDeleteElementFromJson(document, jsonMap);
        } else if (isModifyEvent(jsonMap)) {
            eventElement = createModifyElementFromJson(document, jsonMap);
        } else if (isRenameEvent(jsonMap)) {
            eventElement = createRenameElementFromJson(document, jsonMap);
        } else if (isSyncEvent(jsonMap)) {
            eventElement = createSyncElementFromJson(document, jsonMap);
        } else {
            eventElement = createAddElementFromJson(document, jsonMap);
        }
        inputElement.appendChild(eventElement);

        // Convert the document to a string
        return documentToString(document);
    }

    /**
     * Creates a new XML document.
     *
     * @return A new Document
     * @throws ParserConfigurationException If there is a configuration error
     */
    private Document createDocument() throws ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.newDocument();
    }

    /**
     * Determines if the JSON map represents a modify event.
     *
     * @param jsonMap The JSON map to check
     * @return true if the map represents a modify event, false otherwise
     */
    private boolean isModifyEvent(Map<String, Object> jsonMap) {
        // Check if the attributes map contains modify-specific structures
        if (jsonMap.containsKey("attributes")) {
            Object attributesObj = jsonMap.get("attributes");
            if (attributesObj instanceof Map) {
                Map<String, Object> attributesMap = (Map<String, Object>) attributesObj;
                // Check if any attribute has add-values, remove-values, or remove-all-values
                for (Object attrValue : attributesMap.values()) {
                    if (attrValue instanceof Map) {
                        Map<String, Object> attrMap = (Map<String, Object>) attrValue;
                        if (attrMap.containsKey("add-values") || 
                            attrMap.containsKey("remove-values") || 
                            attrMap.containsKey("remove-all-values")) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Determines if the JSON map represents a delete event.
     *
     * @param jsonMap The JSON map to check
     * @return true if the map represents a delete event, false otherwise
     */
    private boolean isDeleteEvent(Map<String, Object> jsonMap) {
        // Delete events typically don't have attributes but have class-name and src-dn
        // The simplest way to identify a delete event is to check if it has no attributes
        // but has the necessary identifiers (class-name, src-dn, etc.)
        return !jsonMap.containsKey("attributes") && 
               jsonMap.containsKey("class-name") && 
               jsonMap.containsKey("src-dn") &&
               !jsonMap.containsKey("from") && // Not a rename event
               !jsonMap.containsKey("status"); // Not a sync event
    }

    /**
     * Determines if the JSON map represents a rename event.
     *
     * @param jsonMap The JSON map to check
     * @return true if the map represents a rename event, false otherwise
     */
    private boolean isRenameEvent(Map<String, Object> jsonMap) {
        // Rename events have from and to elements
        return jsonMap.containsKey("from") && jsonMap.containsKey("to");
    }

    /**
     * Determines if the JSON map represents a sync event.
     *
     * @param jsonMap The JSON map to check
     * @return true if the map represents a sync event, false otherwise
     */
    private boolean isSyncEvent(Map<String, Object> jsonMap) {
        // Sync events typically have a status element
        return jsonMap.containsKey("status");
    }

    /**
     * Creates a rename element from a JSON map.
     *
     * @param document The XML document
     * @param jsonMap The JSON map representing the rename element
     * @return The created rename element
     */
    private Element createRenameElementFromJson(Document document, Map<String, Object> jsonMap) {
        Element renameElement = document.createElement("rename");

        // Add attributes to the rename element
        if (jsonMap.containsKey("class-name")) {
            renameElement.setAttribute("class-name", jsonMap.get("class-name").toString());
        }
        if (jsonMap.containsKey("event-id")) {
            renameElement.setAttribute("event-id", jsonMap.get("event-id").toString());
        }
        if (jsonMap.containsKey("src-dn")) {
            renameElement.setAttribute("src-dn", jsonMap.get("src-dn").toString());
        }
        if (jsonMap.containsKey("src-entry-id")) {
            renameElement.setAttribute("src-entry-id", jsonMap.get("src-entry-id").toString());
        }

        // Process association if present
        if (jsonMap.containsKey("association")) {
            Object associationObj = jsonMap.get("association");
            if (associationObj instanceof Map) {
                Map<String, Object> associationMap = (Map<String, Object>) associationObj;
                Element associationElement = document.createElement("association");

                // Add attributes to association element
                for (Map.Entry<String, Object> entry : associationMap.entrySet()) {
                    if (!entry.getKey().equals("value")) {
                        associationElement.setAttribute(entry.getKey(), entry.getValue().toString());
                    }
                }

                // Add text content if present
                if (associationMap.containsKey("value")) {
                    associationElement.setTextContent(associationMap.get("value").toString());
                }

                renameElement.appendChild(associationElement);
            }
        }

        // Process from element if present
        if (jsonMap.containsKey("from")) {
            Element fromElement = document.createElement("from");
            fromElement.setTextContent(jsonMap.get("from").toString());
            renameElement.appendChild(fromElement);
        }

        // Process to element if present
        if (jsonMap.containsKey("to")) {
            Element toElement = document.createElement("to");
            toElement.setTextContent(jsonMap.get("to").toString());
            renameElement.appendChild(toElement);
        }

        // Process operation data if present
        if (jsonMap.containsKey("operationData")) {
            Object operationDataObj = jsonMap.get("operationData");
            if (operationDataObj instanceof Map) {
                Map<String, Object> operationDataMap = (Map<String, Object>) operationDataObj;
                Element operationDataElement = document.createElement("operation-data");

                for (Map.Entry<String, Object> entry : operationDataMap.entrySet()) {
                    Element childElement = document.createElement(entry.getKey());
                    childElement.setTextContent(entry.getValue().toString());
                    operationDataElement.appendChild(childElement);
                }

                renameElement.appendChild(operationDataElement);
            }
        }

        return renameElement;
    }

    /**
     * Creates a sync element from a JSON map.
     *
     * @param document The XML document
     * @param jsonMap The JSON map representing the sync element
     * @return The created sync element
     */
    private Element createSyncElementFromJson(Document document, Map<String, Object> jsonMap) {
        Element syncElement = document.createElement("sync");

        // Add attributes to the sync element
        if (jsonMap.containsKey("class-name")) {
            syncElement.setAttribute("class-name", jsonMap.get("class-name").toString());
        }
        if (jsonMap.containsKey("event-id")) {
            syncElement.setAttribute("event-id", jsonMap.get("event-id").toString());
        }
        if (jsonMap.containsKey("src-dn")) {
            syncElement.setAttribute("src-dn", jsonMap.get("src-dn").toString());
        }
        if (jsonMap.containsKey("src-entry-id")) {
            syncElement.setAttribute("src-entry-id", jsonMap.get("src-entry-id").toString());
        }

        // Process association if present
        if (jsonMap.containsKey("association")) {
            Object associationObj = jsonMap.get("association");
            if (associationObj instanceof Map) {
                Map<String, Object> associationMap = (Map<String, Object>) associationObj;
                Element associationElement = document.createElement("association");

                // Add attributes to association element
                for (Map.Entry<String, Object> entry : associationMap.entrySet()) {
                    if (!entry.getKey().equals("value")) {
                        associationElement.setAttribute(entry.getKey(), entry.getValue().toString());
                    }
                }

                // Add text content if present
                if (associationMap.containsKey("value")) {
                    associationElement.setTextContent(associationMap.get("value").toString());
                }

                syncElement.appendChild(associationElement);
            }
        }

        // Process attributes if present
        if (jsonMap.containsKey("attributes")) {
            Object attributesObj = jsonMap.get("attributes");
            if (attributesObj instanceof Map) {
                Map<String, Object> attributesMap = (Map<String, Object>) attributesObj;

                for (Map.Entry<String, Object> entry : attributesMap.entrySet()) {
                    String attrName = entry.getKey();
                    Object attrValue = entry.getValue();

                    Element addAttrElement = document.createElement("add-attr");
                    addAttrElement.setAttribute("attr-name", attrName);

                    if (attrValue instanceof List) {
                        // Multiple values
                        List<Object> valuesList = (List<Object>) attrValue;
                        for (Object valueObj : valuesList) {
                            Element valueElement = createValueElement(document, valueObj);
                            addAttrElement.appendChild(valueElement);
                        }
                    } else {
                        // Single value
                        Element valueElement = createValueElement(document, attrValue);
                        addAttrElement.appendChild(valueElement);
                    }

                    syncElement.appendChild(addAttrElement);
                }
            }
        }

        // Process status element if present
        if (jsonMap.containsKey("status")) {
            Element statusElement = document.createElement("status");
            statusElement.setTextContent(jsonMap.get("status").toString());
            syncElement.appendChild(statusElement);
        }

        // Process operation data if present
        if (jsonMap.containsKey("operationData")) {
            Object operationDataObj = jsonMap.get("operationData");
            if (operationDataObj instanceof Map) {
                Map<String, Object> operationDataMap = (Map<String, Object>) operationDataObj;
                Element operationDataElement = document.createElement("operation-data");

                for (Map.Entry<String, Object> entry : operationDataMap.entrySet()) {
                    Element childElement = document.createElement(entry.getKey());
                    childElement.setTextContent(entry.getValue().toString());
                    operationDataElement.appendChild(childElement);
                }

                syncElement.appendChild(operationDataElement);
            }
        }

        return syncElement;
    }

    /**
     * Creates a delete element from a JSON map.
     *
     * @param document The XML document
     * @param jsonMap The JSON map representing the delete element
     * @return The created delete element
     */
    private Element createDeleteElementFromJson(Document document, Map<String, Object> jsonMap) {
        Element deleteElement = document.createElement("delete");

        // Add attributes to the delete element
        if (jsonMap.containsKey("class-name")) {
            deleteElement.setAttribute("class-name", jsonMap.get("class-name").toString());
        }
        if (jsonMap.containsKey("event-id")) {
            deleteElement.setAttribute("event-id", jsonMap.get("event-id").toString());
        }
        if (jsonMap.containsKey("src-dn")) {
            deleteElement.setAttribute("src-dn", jsonMap.get("src-dn").toString());
        }
        if (jsonMap.containsKey("src-entry-id")) {
            deleteElement.setAttribute("src-entry-id", jsonMap.get("src-entry-id").toString());
        }

        // Process association if present
        if (jsonMap.containsKey("association")) {
            Object associationObj = jsonMap.get("association");
            if (associationObj instanceof Map) {
                Map<String, Object> associationMap = (Map<String, Object>) associationObj;
                Element associationElement = document.createElement("association");

                // Add attributes to association element
                for (Map.Entry<String, Object> entry : associationMap.entrySet()) {
                    if (!entry.getKey().equals("value")) {
                        associationElement.setAttribute(entry.getKey(), entry.getValue().toString());
                    }
                }

                // Add text content if present
                if (associationMap.containsKey("value")) {
                    associationElement.setTextContent(associationMap.get("value").toString());
                }

                deleteElement.appendChild(associationElement);
            }
        }

        // Process operation data if present
        if (jsonMap.containsKey("operationData")) {
            Object operationDataObj = jsonMap.get("operationData");
            if (operationDataObj instanceof Map) {
                Map<String, Object> operationDataMap = (Map<String, Object>) operationDataObj;
                Element operationDataElement = document.createElement("operation-data");

                for (Map.Entry<String, Object> entry : operationDataMap.entrySet()) {
                    Element childElement = document.createElement(entry.getKey());
                    childElement.setTextContent(entry.getValue().toString());
                    operationDataElement.appendChild(childElement);
                }

                deleteElement.appendChild(operationDataElement);
            }
        }

        return deleteElement;
    }

    /**
     * Creates a modify element from a JSON map.
     *
     * @param document The XML document
     * @param jsonMap The JSON map representing the modify element
     * @return The created modify element
     */
    private Element createModifyElementFromJson(Document document, Map<String, Object> jsonMap) {
        Element modifyElement = document.createElement("modify");

        // Add attributes to the modify element
        if (jsonMap.containsKey("class-name")) {
            modifyElement.setAttribute("class-name", jsonMap.get("class-name").toString());
        }
        if (jsonMap.containsKey("event-id")) {
            modifyElement.setAttribute("event-id", jsonMap.get("event-id").toString());
        }
        if (jsonMap.containsKey("src-dn")) {
            modifyElement.setAttribute("src-dn", jsonMap.get("src-dn").toString());
        }
        if (jsonMap.containsKey("src-entry-id")) {
            modifyElement.setAttribute("src-entry-id", jsonMap.get("src-entry-id").toString());
        }

        // Process association if present
        if (jsonMap.containsKey("association")) {
            Object associationObj = jsonMap.get("association");
            if (associationObj instanceof Map) {
                Map<String, Object> associationMap = (Map<String, Object>) associationObj;
                Element associationElement = document.createElement("association");

                // Add attributes to association element
                for (Map.Entry<String, Object> entry : associationMap.entrySet()) {
                    if (!entry.getKey().equals("value")) {
                        associationElement.setAttribute(entry.getKey(), entry.getValue().toString());
                    }
                }

                // Add text content if present
                if (associationMap.containsKey("value")) {
                    associationElement.setTextContent(associationMap.get("value").toString());
                }

                modifyElement.appendChild(associationElement);
            }
        }

        // Process attributes if present
        if (jsonMap.containsKey("attributes")) {
            Object attributesObj = jsonMap.get("attributes");
            if (attributesObj instanceof Map) {
                Map<String, Object> attributesMap = (Map<String, Object>) attributesObj;

                for (Map.Entry<String, Object> entry : attributesMap.entrySet()) {
                    String attrName = entry.getKey();
                    Object attrValue = entry.getValue();

                    if (attrValue instanceof Map) {
                        Map<String, Object> attrModificationMap = (Map<String, Object>) attrValue;
                        Element modifyAttrElement = document.createElement("modify-attr");
                        modifyAttrElement.setAttribute("attr-name", attrName);

                        // Process remove-all-values if present
                        if (attrModificationMap.containsKey("remove-all-values") && 
                            Boolean.TRUE.equals(attrModificationMap.get("remove-all-values"))) {
                            Element removeAllValuesElement = document.createElement("remove-all-values");
                            modifyAttrElement.appendChild(removeAllValuesElement);
                        }

                        // Process remove-values if present
                        if (attrModificationMap.containsKey("remove-values")) {
                            Object removeValuesObj = attrModificationMap.get("remove-values");
                            if (removeValuesObj instanceof List) {
                                List<Object> removeValuesList = (List<Object>) removeValuesObj;
                                for (Object valueObj : removeValuesList) {
                                    Element removeValueElement = document.createElement("remove-value");
                                    Element valueElement = createValueElement(document, valueObj);
                                    removeValueElement.appendChild(valueElement);
                                    modifyAttrElement.appendChild(removeValueElement);
                                }
                            }
                        }

                        // Process add-values if present
                        if (attrModificationMap.containsKey("add-values")) {
                            Object addValuesObj = attrModificationMap.get("add-values");
                            if (addValuesObj instanceof List) {
                                List<Object> addValuesList = (List<Object>) addValuesObj;
                                for (Object valueObj : addValuesList) {
                                    Element addValueElement = document.createElement("add-value");
                                    Element valueElement = createValueElement(document, valueObj);
                                    addValueElement.appendChild(valueElement);
                                    modifyAttrElement.appendChild(addValueElement);
                                }
                            }
                        }

                        modifyElement.appendChild(modifyAttrElement);
                    }
                }
            }
        }

        // Process password if present
        if (jsonMap.containsKey("password")) {
            Element passwordElement = document.createElement("password");
            passwordElement.setTextContent(jsonMap.get("password").toString());
            modifyElement.appendChild(passwordElement);
        }

        // Process operation data if present
        if (jsonMap.containsKey("operationData")) {
            Object operationDataObj = jsonMap.get("operationData");
            if (operationDataObj instanceof Map) {
                Map<String, Object> operationDataMap = (Map<String, Object>) operationDataObj;
                Element operationDataElement = document.createElement("operation-data");

                for (Map.Entry<String, Object> entry : operationDataMap.entrySet()) {
                    Element childElement = document.createElement(entry.getKey());
                    childElement.setTextContent(entry.getValue().toString());
                    operationDataElement.appendChild(childElement);
                }

                modifyElement.appendChild(operationDataElement);
            }
        }

        return modifyElement;
    }

    /**
     * Creates an add element from a JSON map.
     *
     * @param document The XML document
     * @param jsonMap The JSON map representing the add element
     * @return The created add element
     */
    private Element createAddElementFromJson(Document document, Map<String, Object> jsonMap) {
        Element addElement = document.createElement("add");

        // Add attributes to the add element
        if (jsonMap.containsKey("class-name")) {
            addElement.setAttribute("class-name", jsonMap.get("class-name").toString());
        }
        if (jsonMap.containsKey("event-id")) {
            addElement.setAttribute("event-id", jsonMap.get("event-id").toString());
        }
        if (jsonMap.containsKey("src-dn")) {
            addElement.setAttribute("src-dn", jsonMap.get("src-dn").toString());
        }
        if (jsonMap.containsKey("src-entry-id")) {
            addElement.setAttribute("src-entry-id", jsonMap.get("src-entry-id").toString());
        }

        // Process association if present
        if (jsonMap.containsKey("association")) {
            Object associationObj = jsonMap.get("association");
            if (associationObj instanceof Map) {
                Map<String, Object> associationMap = (Map<String, Object>) associationObj;
                Element associationElement = document.createElement("association");

                // Add attributes to association element
                for (Map.Entry<String, Object> entry : associationMap.entrySet()) {
                    if (!entry.getKey().equals("value")) {
                        associationElement.setAttribute(entry.getKey(), entry.getValue().toString());
                    }
                }

                // Add text content if present
                if (associationMap.containsKey("value")) {
                    associationElement.setTextContent(associationMap.get("value").toString());
                }

                addElement.appendChild(associationElement);
            }
        }

        // Process attributes if present
        if (jsonMap.containsKey("attributes")) {
            Object attributesObj = jsonMap.get("attributes");
            if (attributesObj instanceof Map) {
                Map<String, Object> attributesMap = (Map<String, Object>) attributesObj;

                for (Map.Entry<String, Object> entry : attributesMap.entrySet()) {
                    String attrName = entry.getKey();
                    Object attrValue = entry.getValue();

                    Element addAttrElement = document.createElement("add-attr");
                    addAttrElement.setAttribute("attr-name", attrName);

                    if (attrValue instanceof List) {
                        // Multiple values
                        List<Object> valuesList = (List<Object>) attrValue;
                        for (Object valueObj : valuesList) {
                            Element valueElement = createValueElement(document, valueObj);
                            addAttrElement.appendChild(valueElement);
                        }
                    } else {
                        // Single value
                        Element valueElement = createValueElement(document, attrValue);
                        addAttrElement.appendChild(valueElement);
                    }

                    addElement.appendChild(addAttrElement);
                }
            }
        }

        // Process password if present
        if (jsonMap.containsKey("password")) {
            Element passwordElement = document.createElement("password");
            passwordElement.setTextContent(jsonMap.get("password").toString());
            addElement.appendChild(passwordElement);
        }

        // Process operation data if present
        if (jsonMap.containsKey("operationData")) {
            Object operationDataObj = jsonMap.get("operationData");
            if (operationDataObj instanceof Map) {
                Map<String, Object> operationDataMap = (Map<String, Object>) operationDataObj;
                Element operationDataElement = document.createElement("operation-data");

                for (Map.Entry<String, Object> entry : operationDataMap.entrySet()) {
                    Element childElement = document.createElement(entry.getKey());
                    childElement.setTextContent(entry.getValue().toString());
                    operationDataElement.appendChild(childElement);
                }

                addElement.appendChild(operationDataElement);
            }
        }

        return addElement;
    }

    /**
     * Creates a value element from a JSON object.
     *
     * @param document The XML document
     * @param valueObj The JSON object representing the value
     * @return The created value element
     */
    private Element createValueElement(Document document, Object valueObj) {
        Element valueElement = document.createElement("value");

        if (valueObj instanceof Map) {
            Map<String, Object> valueMap = (Map<String, Object>) valueObj;

            // Add attributes to value element
            for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
                if (!entry.getKey().equals("value") && !entry.getKey().equals("components")) {
                    valueElement.setAttribute(entry.getKey(), entry.getValue().toString());
                }
            }

            // Process components if present
            if (valueMap.containsKey("components")) {
                Object componentsObj = valueMap.get("components");
                if (componentsObj instanceof Map) {
                    Map<String, Object> componentsMap = (Map<String, Object>) componentsObj;

                    for (Map.Entry<String, Object> entry : componentsMap.entrySet()) {
                        Element componentElement = document.createElement("component");
                        componentElement.setAttribute("name", entry.getKey());
                        componentElement.setTextContent(entry.getValue().toString());
                        valueElement.appendChild(componentElement);
                    }
                }
            } else if (valueMap.containsKey("value")) {
                // Simple value with attributes
                valueElement.setTextContent(valueMap.get("value").toString());
            }
        } else {
            // Simple value without attributes
            valueElement.setTextContent(valueObj.toString());
        }

        return valueElement;
    }

    /**
     * Converts a Document to a string.
     *
     * @param document The Document to convert
     * @return The XML string representation of the Document
     * @throws TransformerException If there is an error during transformation
     */
    private String documentToString(Document document) throws TransformerException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

        DOMSource source = new DOMSource(document);
        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);
        transformer.transform(source, result);

        return writer.toString();
    }

    /**
     * Parses a JSON string into a Map.
     *
     * @param jsonString The JSON string to parse
     * @return A Map representing the JSON object
     * @throws IOException If there is an error during parsing
     */
    private Map<String, Object> parseJsonString(String jsonString) throws IOException {
        // Remove leading/trailing whitespace
        jsonString = jsonString.trim();

        // Check if the string starts with a curly brace (object)
        if (!jsonString.startsWith("{") || !jsonString.endsWith("}")) {
            throw new IOException("Invalid JSON object: must start with '{' and end with '}'");
        }

        // Remove the outer braces
        jsonString = jsonString.substring(1, jsonString.length() - 1).trim();

        Map<String, Object> result = new LinkedHashMap<>();

        // Parse the key-value pairs
        int pos = 0;
        while (pos < jsonString.length()) {
            // Skip whitespace
            while (pos < jsonString.length() && Character.isWhitespace(jsonString.charAt(pos))) {
                pos++;
            }

            if (pos >= jsonString.length()) {
                break;
            }

            // Parse the key (should be in quotes)
            if (jsonString.charAt(pos) != '"') {
                throw new IOException("Expected '\"' at position " + pos);
            }

            int keyStart = pos + 1;
            pos = findClosingQuote(jsonString, keyStart);
            String key = jsonString.substring(keyStart, pos);
            pos++; // Skip the closing quote

            // Skip whitespace and the colon
            while (pos < jsonString.length() && (Character.isWhitespace(jsonString.charAt(pos)) || jsonString.charAt(pos) == ':')) {
                pos++;
            }

            // Parse the value
            Object value;
            if (jsonString.charAt(pos) == '{') {
                // Nested object
                int objStart = pos;
                pos = findClosingBrace(jsonString, objStart);
                String objStr = jsonString.substring(objStart, pos + 1);
                value = parseJsonString(objStr);
                pos++; // Skip the closing brace
            } else if (jsonString.charAt(pos) == '[') {
                // Array
                int arrayStart = pos;
                pos = findClosingBracket(jsonString, arrayStart);
                String arrayStr = jsonString.substring(arrayStart + 1, pos);
                value = parseJsonArray(arrayStr);
                pos++; // Skip the closing bracket
            } else if (jsonString.charAt(pos) == '"') {
                // String
                int strStart = pos + 1;
                pos = findClosingQuote(jsonString, strStart);
                value = jsonString.substring(strStart, pos);
                pos++; // Skip the closing quote
            } else {
                // Number, boolean, or null
                int valueStart = pos;
                while (pos < jsonString.length() && 
                       !Character.isWhitespace(jsonString.charAt(pos)) && 
                       jsonString.charAt(pos) != ',' && 
                       jsonString.charAt(pos) != '}') {
                    pos++;
                }
                String valueStr = jsonString.substring(valueStart, pos);
                if (valueStr.equals("null")) {
                    value = null;
                } else if (valueStr.equals("true")) {
                    value = Boolean.TRUE;
                } else if (valueStr.equals("false")) {
                    value = Boolean.FALSE;
                } else {
                    try {
                        value = Integer.parseInt(valueStr);
                    } catch (NumberFormatException e1) {
                        try {
                            value = Double.parseDouble(valueStr);
                        } catch (NumberFormatException e2) {
                            value = valueStr;
                        }
                    }
                }
            }

            result.put(key, value);

            // Skip whitespace and the comma
            while (pos < jsonString.length() && (Character.isWhitespace(jsonString.charAt(pos)) || jsonString.charAt(pos) == ',')) {
                pos++;
            }
        }

        return result;
    }

    /**
     * Parses a JSON array string into a List.
     *
     * @param arrayStr The JSON array string to parse (without the outer brackets)
     * @return A List representing the JSON array
     * @throws IOException If there is an error during parsing
     */
    private List<Object> parseJsonArray(String arrayStr) throws IOException {
        List<Object> result = new ArrayList<>();

        // Remove leading/trailing whitespace
        arrayStr = arrayStr.trim();

        if (arrayStr.isEmpty()) {
            return result;
        }

        int pos = 0;
        while (pos < arrayStr.length()) {
            // Skip whitespace
            while (pos < arrayStr.length() && Character.isWhitespace(arrayStr.charAt(pos))) {
                pos++;
            }

            if (pos >= arrayStr.length()) {
                break;
            }

            // Parse the value
            Object value;
            if (arrayStr.charAt(pos) == '{') {
                // Nested object
                int objStart = pos;
                pos = findClosingBrace(arrayStr, objStart);
                String objStr = arrayStr.substring(objStart, pos + 1);
                value = parseJsonString(objStr);
                pos++; // Skip the closing brace
            } else if (arrayStr.charAt(pos) == '[') {
                // Nested array
                int arrayStart = pos;
                pos = findClosingBracket(arrayStr, arrayStart);
                String nestedArrayStr = arrayStr.substring(arrayStart + 1, pos);
                value = parseJsonArray(nestedArrayStr);
                pos++; // Skip the closing bracket
            } else if (arrayStr.charAt(pos) == '"') {
                // String
                int strStart = pos + 1;
                pos = findClosingQuote(arrayStr, strStart);
                value = arrayStr.substring(strStart, pos);
                pos++; // Skip the closing quote
            } else {
                // Number, boolean, or null
                int valueStart = pos;
                while (pos < arrayStr.length() && 
                       !Character.isWhitespace(arrayStr.charAt(pos)) && 
                       arrayStr.charAt(pos) != ',' && 
                       arrayStr.charAt(pos) != ']') {
                    pos++;
                }
                String valueStr = arrayStr.substring(valueStart, pos);
                if (valueStr.equals("null")) {
                    value = null;
                } else if (valueStr.equals("true")) {
                    value = Boolean.TRUE;
                } else if (valueStr.equals("false")) {
                    value = Boolean.FALSE;
                } else {
                    try {
                        value = Integer.parseInt(valueStr);
                    } catch (NumberFormatException e1) {
                        try {
                            value = Double.parseDouble(valueStr);
                        } catch (NumberFormatException e2) {
                            value = valueStr;
                        }
                    }
                }
            }

            result.add(value);

            // Skip whitespace and the comma
            while (pos < arrayStr.length() && (Character.isWhitespace(arrayStr.charAt(pos)) || arrayStr.charAt(pos) == ',')) {
                pos++;
            }
        }

        return result;
    }

    /**
     * Finds the position of the closing quote in a JSON string.
     *
     * @param str The JSON string
     * @param start The position after the opening quote
     * @return The position of the closing quote
     * @throws IOException If there is no closing quote
     */
    private int findClosingQuote(String str, int start) throws IOException {
        int pos = start;
        while (pos < str.length()) {
            if (str.charAt(pos) == '\\') {
                pos += 2; // Skip the escape sequence
            } else if (str.charAt(pos) == '"') {
                return pos;
            } else {
                pos++;
            }
        }
        throw new IOException("No closing quote found");
    }

    /**
     * Finds the position of the closing brace in a JSON object.
     *
     * @param str The JSON string
     * @param start The position of the opening brace
     * @return The position of the closing brace
     * @throws IOException If there is no closing brace
     */
    private int findClosingBrace(String str, int start) throws IOException {
        int pos = start + 1;
        int depth = 1;

        while (pos < str.length() && depth > 0) {
            char c = str.charAt(pos);
            if (c == '"') {
                pos = findClosingQuote(str, pos + 1) + 1;
            } else if (c == '{') {
                depth++;
                pos++;
            } else if (c == '}') {
                depth--;
                pos++;
            } else {
                pos++;
            }
        }

        if (depth != 0) {
            throw new IOException("No closing brace found");
        }

        return pos - 1;
    }

    /**
     * Finds the position of the closing bracket in a JSON array.
     *
     * @param str The JSON string
     * @param start The position of the opening bracket
     * @return The position of the closing bracket
     * @throws IOException If there is no closing bracket
     */
    private int findClosingBracket(String str, int start) throws IOException {
        int pos = start + 1;
        int depth = 1;

        while (pos < str.length() && depth > 0) {
            char c = str.charAt(pos);
            if (c == '"') {
                pos = findClosingQuote(str, pos + 1) + 1;
            } else if (c == '[') {
                depth++;
                pos++;
            } else if (c == ']') {
                depth--;
                pos++;
            } else {
                pos++;
            }
        }

        if (depth != 0) {
            throw new IOException("No closing bracket found");
        }

        return pos - 1;
    }

    /**
     * Main method for testing the converter.
     *
     * @param args Command line arguments (not used)
     */
    public static void main(String[] args) {
        try {
            // First convert XML to JSON using AddEventConverter
            AddEventConverter xmlToJsonConverter = new AddEventConverter();
            String exampleXml = "<nds dtdversion=\"4.0\" ndsversion=\"8.x\" xmlns:cmd=\"http://www.novell.com/nxsl/java/com.novell.nds.dirxml.driver.XdsCommandProcessor\" xmlns:jstring=\"http://www.novell.com/nxsl/java/java.lang.String\" xmlns:query=\"http://www.novell.com/nxsl/java/com.novell.nds.dirxml.driver.XdsQueryProcessor\">\n" +
                    "    <source>\n" +
                    "        <product version=\"1.0\">DirXML</product>\n" +
                    "        <contact>Novell, Inc.</contact>\n" +
                    "    </source>\n" +
                    "\n" +
                    "    <input>\n" +
                    "        <add class-name=\"user\" event-id=\"0\" src-dn=\"\\PERIN-TAO\\novell\\John\" src-entry-id=\"35868\">\n" +
                    "            <add-attr attr-name=\"username\">\n" +
                    "                <value timestamp=\"965252229#1\" type=\"string\">Z123483</value>\n" +
                    "            </add-attr>\n" +
                    "            <add-attr attr-name=\"firstName\">\n" +
                    "                <value timestamp=\"965252229#1\" type=\"string\">Frank</value>\n" +
                    "            </add-attr>\n" +
                    "        </add>\n" +
                    "    </input>\n" +
                    "</nds>";

            String jsonResult = xmlToJsonConverter.convertToJson(exampleXml);
            System.out.println("Converted to JSON:");
            System.out.println(jsonResult);

            // Then convert JSON back to XML using JsonToXmlConverter
            JsonToXmlConverter jsonToXmlConverter = new JsonToXmlConverter();
            String xmlResult = jsonToXmlConverter.convertToXml(jsonResult);
            System.out.println("\nConverted back to XML:");
            System.out.println(xmlResult);
        } catch (Exception e) {
            System.err.println("Error during conversion: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
