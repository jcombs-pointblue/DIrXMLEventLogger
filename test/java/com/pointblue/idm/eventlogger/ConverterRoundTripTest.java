package com.pointblue.idm.eventlogger;

import com.pointblue.idm.eventlogger.xds2json.JsonToXmlConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * XML -> JSON -> XML through the event converters and {@link JsonToXmlConverter}.
 * The event element must come back with the same elements, attributes and text.
 * Sibling order is ignored (XDS does not depend on it within these elements), and so is
 * the {@code <nds>}/{@code <input>} wrapper, which the JSON does not keep.
 */
class ConverterRoundTripTest {

    static String fixture(String type) throws Exception {
        try (InputStream in = ConverterRoundTripTest.class.getResourceAsStream("/roundtrip/" + type + ".xml")) {
            assertNotNull(in, "missing fixture " + type);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    static Element event(String xml, String type) throws Exception {
        Element e = (Element) XmlSupport.parse(xml).getElementsByTagName(type).item(0);
        assertNotNull(e, "no <" + type + "> in " + xml);
        return e;
    }

    /** Order-independent canonical form: name, sorted attributes, trimmed text, sorted children. */
    static String canonical(Element element) {
        Map<String, String> attrs = new TreeMap<>();
        for (int i = 0; i < element.getAttributes().getLength(); i++) {
            Node a = element.getAttributes().item(i);
            attrs.put(a.getNodeName(), a.getNodeValue());
        }
        List<String> children = new ArrayList<>();
        for (Node c = element.getFirstChild(); c != null; c = c.getNextSibling()) {
            if (c instanceof Element) {
                children.add(canonical((Element) c));
            }
        }
        children.sort(null);
        String text = children.isEmpty() ? element.getTextContent().trim() : "";
        return "<" + element.getNodeName() + " " + attrs + ">" + text + children + "</" + element.getNodeName() + ">";
    }

    @ParameterizedTest
    @ValueSource(strings = {"add", "modify", "delete", "sync", "rename", "move"})
    void xmlToJsonToXmlIsEquivalent(String type) throws Exception {
        String original = fixture(type);
        String json = EventRecords.converterFor(type).convertToJson(original);
        String rebuilt = new JsonToXmlConverter().convertToXml(json);

        assertEquals(canonical(event(original, type)), canonical(event(rebuilt, type)),
                "round trip changed the event.\nJSON:\n" + json + "\nRebuilt:\n" + rebuilt);
    }

    @Test
    void schemaVersionAndPolicyKeysDoNotBecomeAttributes() throws Exception {
        String rebuilt = new JsonToXmlConverter().convertToXml(
                "{\"event-type\":\"delete\",\"schemaVersion\":2,\"class-name\":\"User\",\"src-dn\":\"\\\\T\\\\u\","
                        + "\"logged-by-policy\":\"P\",\"logged-channel\":\"sub\"}");
        Element delete = event(rebuilt, "delete");
        assertEquals("<delete {class-name=User, src-dn=\\T\\u}>[]</delete>", canonical(delete));
    }

    @Test
    void unknownEventTypeIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new JsonToXmlConverter().convertToXml("{\"event-type\":\"query\"}"));
    }
}
