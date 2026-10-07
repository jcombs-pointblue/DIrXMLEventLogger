package com.novell.nds.dirxml.driver;

// Compile-only stand-in for the Identity Manager driver API (see idm-api-stubs/README.md).
// Never packaged: the engine supplies the real class at runtime.

public class XmlDocument {
    public XmlDocument(org.w3c.dom.Document document) {
        throw new UnsupportedOperationException("compile-only stub");
    }

    public org.w3c.dom.Document getDocument() {
        throw new UnsupportedOperationException("compile-only stub");
    }

    public org.w3c.dom.Document getDocumentNS() {
        throw new UnsupportedOperationException("compile-only stub");
    }

    public String getDocumentString() {
        throw new UnsupportedOperationException("compile-only stub");
    }
}
