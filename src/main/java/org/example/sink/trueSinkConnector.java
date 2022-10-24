package org.example.sink;

import org.example.common.ConnectorVersion;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class trueSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(trueSinkConnector.class);

    trueSinkConfig config;

    @Override
    public void start(Map<String, String> props) {
        LOGGER.debug("Starting connector");
        this.config = new trueSinkConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return trueSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.debug("Requested task configurations for {} tasks", maxTasks);

        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(new HashMap<>(config.originalsStrings()));
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        LOGGER.debug("Stopping connector");
    }

    @Override
    public ConfigDef config() {
        return trueSinkConfig.configDef();
    }

    @Override
    public String version() {
        return ConnectorVersion.getVersion();
    }
}
