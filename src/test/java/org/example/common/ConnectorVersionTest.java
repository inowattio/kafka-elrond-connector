package org.example.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectorVersionTest {
    // The same value should be in the test resource connector.properties
    public static final String EXPECTED_VERSION = "testing";

    @Test
    void getVersion() {
        assertEquals(EXPECTED_VERSION, ConnectorVersion.getVersion());
    }

}