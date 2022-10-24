package org.example.transformation;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class trueTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(trueTransformation.class);
    private trueTransformationConfig config;

    @Override
    public R apply(R record) {
        LOGGER.debug("Applying transformation");
        return null;
    }

    @Override
    public ConfigDef config() {
        return trueTransformationConfig.configDef();
    }

    @Override
    public void close() {
        LOGGER.debug("Closing transformation");
        // Close any resource you might have open
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOGGER.debug("Configuring transformation");
        config = new trueTransformationConfig(configs);
    }
}
