package com.novell.nds.dirxml.driver;

// Compile-only stand-in for the Identity Manager driver API (see idm-api-stubs/README.md).
// Never packaged: the engine supplies the real class at runtime.

public interface SubscriptionShim {
    XmlDocument init(XmlDocument initParameters);

    XmlDocument execute(XmlDocument doc, XmlQueryProcessor query);
}
