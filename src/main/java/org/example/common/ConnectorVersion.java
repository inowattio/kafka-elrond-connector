package org.example.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConnectorVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorVersion.class);
    private static final String INFO_PATH = "/connector.properties";
    private static final String CURRENT_VERSION;

    static {
        Properties properties = new Properties();
        try (InputStream stream = ConnectorVersion.class.getResourceAsStream(INFO_PATH)) {
            properties.load(stream);
        } catch (IOException e) {
            LOGGER.warn("Could not read info resource file {}", INFO_PATH, e);
        }
        CURRENT_VERSION = properties.getProperty("version", "unknown");
    }

    private ConnectorVersion() {
        // Private constructor to prevent instantiation
    }

    /**
     * Get the current connector version
     *
     * @return the connector version
     */
    public static String getVersion() {
        return CURRENT_VERSION;
    }
}
