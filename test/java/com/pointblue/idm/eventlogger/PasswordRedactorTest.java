package com.pointblue.idm.eventlogger;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class PasswordRedactorTest {

    static String resource(String name) throws Exception {
        try (InputStream in = PasswordRedactorTest.class.getResourceAsStream("/events/" + name)) {
            assertNotNull(in, "missing test resource " + name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void addEventCarriesOnlyTheMarkerInJsonAndXml() throws Exception {
        EventRecord record = EventRecords.prepare(resource("add-with-passwords.xml"));

        for (String stored : new String[]{record.xml, record.json.toString()}) {
            assertFalse(stored.contains("S3cret"), "password leaked: " + stored);
            assertTrue(stored.contains(PasswordRedactor.MARKER));
        }
        assertEquals(PasswordRedactor.MARKER, record.json.getString("password"));
        assertEquals(PasswordRedactor.MARKER, record.json.getJSONObject("attributes").getJSONObject("nspmDistributionPassword").getString("value"));
        assertEquals(PasswordRedactor.MARKER, record.json.getJSONObject("operationData").getString("saved-Password"));
        // Non-password data is untouched
        assertEquals("Doe", record.json.getJSONObject("attributes").getJSONObject("Surname").getString("value"));
        assertEquals("abc-123", record.json.getJSONObject("operationData").getString("correlation"));
    }

    @Test
    void modifyPasswordMasksOldAndNew() throws Exception {
        String xml = "<nds><input><modify-password class-name=\"User\" src-dn=\"\\T\\u\">"
                + "<old-password>Old1</old-password><password>New1</password></modify-password></input></nds>";
        String redacted = PasswordRedactor.redact(xml);
        assertFalse(redacted.contains("Old1") || redacted.contains("New1"), redacted);
        assertTrue(redacted.contains("<old-password>***</old-password>"), redacted);
    }

    @Test
    void checkObjectPasswordIsMasked() throws Exception {
        String redacted = PasswordRedactor.redact(
                "<nds><input><check-object-password dest-dn=\"\\T\\u\"><password>Pw9</password></check-object-password></input></nds>");
        assertFalse(redacted.contains("Pw9"), redacted);
    }

    @Test
    void modifyAttrPasswordValuesAreMaskedCaseInsensitively() throws Exception {
        String redacted = PasswordRedactor.redact(
                "<nds><input><modify class-name=\"User\"><modify-attr attr-name=\"NSPMDISTRIBUTIONPASSWORD\">"
                        + "<remove-value><value>OldPw</value></remove-value><add-value><value>NewPw</value></add-value>"
                        + "</modify-attr></modify></input></nds>");
        assertFalse(redacted.contains("OldPw") || redacted.contains("NewPw"), redacted);
    }

    @Test
    void documentWithoutPasswordsIsReturnedUnchanged() throws Exception {
        String xml = "<nds><input><delete class-name=\"User\" src-dn=\"\\T\\u\"/></input></nds>";
        assertSame(xml, PasswordRedactor.redact(xml));
    }
}
