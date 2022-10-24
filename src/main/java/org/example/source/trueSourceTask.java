package org.example.source;

import org.example.common.ConnectorVersion;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class trueSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(trueSourceTask.class);
    private trueSourceConfig config;

    @Override
    public String version() {
        return ConnectorVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.debug("Starting task");
        config = new trueSourceConfig(props);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LOGGER.debug("Polling for records");
        return Collections.emptyList();
    }

    @Override
    public void stop() {
        LOGGER.debug("Stopping task");

    }
}
