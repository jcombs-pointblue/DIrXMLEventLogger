package com.pointblue.idm.eventlogger;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.Locale;

/**
 * Masks password values in an XDS document before it is converted or stored.
 * <p>
 * Masked, with every value replaced by {@link #MARKER}:
 * <ul>
 *   <li>The text of any element whose name contains "password" (case-insensitive):
 *       {@code <password>}, {@code <old-password>}, the children of
 *       {@code <modify-password>} and {@code <check-object-password>}, and password
 *       elements inside {@code <operation-data>}</li>
 *   <li>Every value of an attribute whose {@code attr-name} contains "password"
 *       (case-insensitive), e.g. {@code nspmDistributionPassword}</li>
 * </ul>
 * Masking happens on the XML, so the JSON converted from it and the stored raw XML
 * can only ever carry the marker.
 */
public final class PasswordRedactor {

    /** Replacement for every redacted value. */
    public static final String MARKER = "***";

    private PasswordRedactor() {
    }

    /**
     * Returns the document with all password values masked. If nothing needed
     * masking, the original string is returned unchanged.
     *
     * @param xml the XDS document
     * @return the redacted XDS document
     * @throws Exception if the XML cannot be parsed
     */
    public static String redact(String xml) throws Exception {
        Document doc = XmlSupport.parse(xml);
        return redact(doc.getDocumentElement()) ? XmlSupport.serialize(doc) : xml;
    }

    /** Masks passwords below {@code element}; returns true if anything changed. */
    private static boolean redact(Element element) {
        if (isPasswordName(element.getAttribute("attr-name"))) {
            return maskValues(element);
        }
        boolean changed = false;
        boolean hasChildElements = false;
        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) instanceof Element) {
                hasChildElements = true;
                changed |= redact((Element) children.item(i));
            }
        }
        if (!hasChildElements && isPasswordName(element.getNodeName())) {
            changed |= mask(element);
        }
        return changed;
    }

    /** Masks every {@code <value>} (or its {@code <component>}s) below an attribute element. */
    private static boolean maskValues(Element attrElement) {
        boolean changed = false;
        NodeList values = attrElement.getElementsByTagName("value");
        for (int i = 0; i < values.getLength(); i++) {
            Element value = (Element) values.item(i);
            NodeList components = value.getElementsByTagName("component");
            if (components.getLength() == 0) {
                changed |= mask(value);
            } else {
                for (int j = 0; j < components.getLength(); j++) {
                    changed |= mask((Element) components.item(j));
                }
            }
        }
        return changed;
    }

    /** Replaces an element's text with the marker; empty elements are left alone. */
    private static boolean mask(Element element) {
        if (element.getTextContent().trim().isEmpty()) {
            return false;
        }
        while (element.getFirstChild() != null) {
            element.removeChild(element.getFirstChild());
        }
        element.appendChild(element.getOwnerDocument().createTextNode(MARKER));
        return true;
    }

    private static boolean isPasswordName(String name) {
        return name != null && name.toLowerCase(Locale.ROOT).contains("password");
    }
}
