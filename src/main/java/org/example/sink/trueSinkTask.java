package org.example.sink;

import org.example.common.ConnectorVersion;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class trueSinkTask extends SinkTask {
    @Override
    public String version() {
        return ConnectorVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public void put(Collection<SinkRecord> records) {

    }

    @Override
    public void stop() {

    }
}
