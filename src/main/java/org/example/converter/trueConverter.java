package org.example.converter;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class trueConverter implements Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(trueConverter.class);
    private trueConverterConfig config;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        LOGGER.debug("Configuring converter, isKey = {}", isKey);
        config = new trueConverterConfig(configs);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return new byte[0];
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return trueConverterConfig.configDef();
    }

}
