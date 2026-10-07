package com.pointblue.idm.eventlogger.xds2json;

import com.pointblue.idm.eventlogger.json.JSONArray;
import com.pointblue.idm.eventlogger.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Rebuilds an XDS document from the JSON the {@code *EventConverter} classes produce.
 * <p>
 * The event type comes from {@code event-type}. Every other top-level string or number
 * becomes an attribute of the event element (class-name, src-dn, event-id, timestamp, ...);
 * the structured keys below become child elements.
 * <p>
 * The result is equivalent, not identical, to the original document. See the README
 * section "What the JSON does not keep" for what is lost.
 */
public class JsonToXmlConverter {

    /** Keys that are not attributes of the event element. */
    private static final Set<String> STRUCTURED_KEYS = new HashSet<>(Arrays.asList(
            "event-type", "schemaVersion", "association", "attributes", "password", "operationData",
            "status", "from", "to", "new-name", "parent", "logged-by-policy", "logged-channel"));

    /**
     * Converts event JSON back to an XDS document.
     *
     * @param jsonString JSON from one of the event converters
     * @return the XDS document as a string
     * @throws IllegalArgumentException if the JSON has no supported {@code event-type}
     * @throws Exception                if the document cannot be built
     */
    public String convertToXml(String jsonString) throws Exception {
        JSONObject json = new JSONObject(jsonString);
        String type = json.optString("event-type", "");
        if (!Arrays.asList("add", "modify", "delete", "sync", "rename", "move").contains(type)) {
            throw new IllegalArgumentException("Unsupported event-type: " + type);
        }

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        Element nds = doc.createElement("nds");
        nds.setAttribute("dtdversion", "4.0");
        nds.setAttribute("ndsversion", "8.x");
        doc.appendChild(nds);
        Element input = append(nds, "input");
        Element event = append(input, type);

        for (String key : new TreeSet<>(json.keySet())) {
            Object value = json.get(key);
            if (!STRUCTURED_KEYS.contains(key) && !(value instanceof JSONObject) && !(value instanceof JSONArray)) {
                event.setAttribute(key, value.toString());
            }
        }

        if (json.has("association")) {
            appendAssociation(event, json.getJSONObject("association"));
        }
        JSONObject attributes = json.optJSONObject("attributes");
        if (attributes != null) {
            if (type.equals("modify")) {
                appendModifyAttrs(event, attributes);
            } else {
                appendAddAttrs(event, attributes);
            }
        }
        if (json.has("new-name")) {
            append(event, "new-name").setTextContent(json.getString("new-name"));
        }
        for (String key : new String[]{"from", "to"}) {
            if (json.has(key)) {
                append(event, key).setTextContent(json.getString(key));
            }
        }
        if (json.has("parent")) {
            JSONObject parentJson = json.getJSONObject("parent");
            Element parent = append(event, "parent");
            setAttributes(parent, parentJson, "association", "value");
            if (parentJson.has("association")) {
                appendAssociation(parent, parentJson.getJSONObject("association"));
            } else if (parentJson.has("value")) {
                parent.setTextContent(parentJson.getString("value")); // schemaVersion 1 shape
            }
        }
        if (json.has("password")) {
            append(event, "password").setTextContent(json.getString("password"));
        }
        if (json.has("status")) {
            append(event, "status").setTextContent(json.getString("status"));
        }
        JSONObject operationData = json.optJSONObject("operationData");
        if (operationData != null) {
            Element opData = append(event, "operation-data");
            for (String key : new TreeSet<>(operationData.keySet())) {
                append(opData, key).setTextContent(operationData.get(key).toString());
            }
        }
        return serialize(doc);
    }

    private static void appendAssociation(Element parent, JSONObject associationJson) {
        Element association = append(parent, "association");
        setAttributes(association, associationJson, "value");
        if (associationJson.has("value")) {
            association.setTextContent(associationJson.getString("value"));
        }
    }

    /** {@code {"attr": value-or-list}} to {@code <add-attr>} elements (add and sync). */
    private static void appendAddAttrs(Element event, JSONObject attributes) {
        for (String name : new TreeSet<>(attributes.keySet())) {
            Element addAttr = append(event, "add-attr");
            addAttr.setAttribute("attr-name", name);
            appendValues(addAttr, attributes.get(name));
        }
    }

    /** {@code {"attr": {"remove-all-values", "remove-values", "add-values"}}} to {@code <modify-attr>}. */
    private static void appendModifyAttrs(Element event, JSONObject attributes) {
        for (String name : new TreeSet<>(attributes.keySet())) {
            JSONObject changes = attributes.getJSONObject(name);
            Element modifyAttr = append(event, "modify-attr");
            modifyAttr.setAttribute("attr-name", name);
            if (changes.optBoolean("remove-all-values", false)) {
                append(modifyAttr, "remove-all-values");
            }
            if (changes.has("remove-values")) {
                appendValues(append(modifyAttr, "remove-value"), changes.get("remove-values"));
            }
            if (changes.has("add-values")) {
                appendValues(append(modifyAttr, "add-value"), changes.get("add-values"));
            }
        }
    }

    /** One value, or a list of them, to {@code <value>} elements. */
    private static void appendValues(Element parent, Object values) {
        if (values instanceof JSONArray) {
            JSONArray list = (JSONArray) values;
            for (int i = 0; i < list.length(); i++) {
                appendValue(parent, list.get(i));
            }
        } else {
            appendValue(parent, values);
        }
    }

    /**
     * A value is a string, {@code {attrs..., "value": text}}, or
     * {@code {attrs..., "components": {name: text}}}.
     */
    private static void appendValue(Element parent, Object valueJson) {
        Element value = append(parent, "value");
        if (!(valueJson instanceof JSONObject)) {
            value.setTextContent(valueJson.toString());
            return;
        }
        JSONObject valueObject = (JSONObject) valueJson;
        setAttributes(value, valueObject, "value", "components");
        JSONObject components = valueObject.optJSONObject("components");
        if (components != null) {
            for (String name : new TreeSet<>(components.keySet())) {
                Element component = append(value, "component");
                component.setAttribute("name", name);
                component.setTextContent(components.get(name).toString());
            }
        } else if (valueObject.has("value")) {
            value.setTextContent(valueObject.get("value").toString());
        }
    }

    /** Sets every scalar key of {@code json} as an attribute, except the excluded keys. */
    private static void setAttributes(Element element, JSONObject json, String... excluded) {
        Set<String> skip = new HashSet<>(Arrays.asList(excluded));
        for (String key : new TreeSet<>(json.keySet())) {
            Object value = json.get(key);
            if (!skip.contains(key) && !(value instanceof JSONObject) && !(value instanceof JSONArray)) {
                element.setAttribute(key, value.toString());
            }
        }
    }

    private static Element append(Element parent, String name) {
        Element child = parent.getOwnerDocument().createElement(name);
        parent.appendChild(child);
        return child;
    }

    private static String serialize(Document doc) throws Exception {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }
}
