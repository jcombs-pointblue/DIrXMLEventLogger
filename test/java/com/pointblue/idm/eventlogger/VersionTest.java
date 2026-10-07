package com.pointblue.idm.eventlogger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VersionTest {

    @Test
    void versionConstantMatchesPom() {
        assertEquals(System.getProperty("project.version"), EventLoggerDriver.VERSION,
                "Update EventLoggerDriver.VERSION when changing the version in pom.xml");
    }
}
