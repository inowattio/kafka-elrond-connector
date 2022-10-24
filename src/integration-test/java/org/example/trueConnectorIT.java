package org.example;

import org.example.sink.trueSinkConnector;
import org.example.source.trueSourceConnector;
import org.example.testtools.client.ConnectClient;
import org.example.testtools.jupiter.ConnectExtension;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.PluginInfo;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class trueConnectorIT {
    private static final String TOPIC_NAME = "testing-topic";
    private static final String SOURCE_CONNECTOR_NAME = "source-conn-testing";
    private static final String SINK_CONNECTOR_NAME = "sink-conn-testing";
    private static final String SOURCE_CONNECTOR_CLASS_NAME = trueSourceConnector.class.getName();
    private static final String SINK_CONNECTOR_CLASS_NAME = trueSinkConnector.class.getName();
    @RegisterExtension
    ConnectExtension connect = new ConnectExtension();
    private ConnectClient connectClient;

    @BeforeEach
    public void setup() {
        connectClient = connect.connectClient();
        connect.createTopic(TOPIC_NAME);
    }

    @Test
    void findConnectors() {
        List<PluginInfo> plugins = connectClient.getAllConnectorPlugins();
        Set<String> classes = plugins.stream().map(PluginInfo::className).collect(Collectors.toSet());
        assertTrue(classes.contains(SOURCE_CONNECTOR_CLASS_NAME), "Missing connector plugin " + SOURCE_CONNECTOR_CLASS_NAME);
        assertTrue(classes.contains(SINK_CONNECTOR_CLASS_NAME), "Missing connector plugin " + SINK_CONNECTOR_CLASS_NAME);
    }

    @Test
    void runSourceConnector() {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.put("connector.class", SOURCE_CONNECTOR_CLASS_NAME);

        final ConnectClient client = connect.connectClient();
        client.createConnector(SOURCE_CONNECTOR_NAME, connectorProperties);


        Awaitility.await("Waiting for connector to start").atMost(10, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(100)).until(() -> isRunning(SOURCE_CONNECTOR_NAME));

        // TODO: Implement functional tests for Source Connector
    }

    @Test
    void runSinkConnector() {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.put("connector.class", SINK_CONNECTOR_CLASS_NAME);
        connectorProperties.put("topics", TOPIC_NAME);

        final ConnectClient client = connect.connectClient();
        client.createConnector(SINK_CONNECTOR_NAME, connectorProperties);

        Awaitility.await("Waiting for connector to start").atMost(10, TimeUnit.SECONDS).pollInterval(Duration.ofMillis(100)).until(() -> isRunning(SINK_CONNECTOR_NAME));

        // TODO: Implement functional tests for Sink Connector
    }

    boolean isRunning(String connectorName) {
        ConnectorStateInfo stateInfo = connectClient.getConnectorStatus(connectorName);
        if (stateInfo == null) {
            return false;
        }

        if (stateInfo.connector() == null || !"RUNNING".equalsIgnoreCase(stateInfo.connector().state())) {
            return false;
        }

        return stateInfo.tasks().stream().allMatch(taskState -> !"RUNNING".equalsIgnoreCase(taskState.state()));
    }
}
