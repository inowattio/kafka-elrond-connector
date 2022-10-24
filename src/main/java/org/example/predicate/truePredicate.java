package org.example.predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class truePredicate<R extends ConnectRecord<R>> implements Predicate<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(truePredicate.class);
    private truePredicateConfig config;

    @Override
    public ConfigDef config() {
        return truePredicateConfig.configDef();
    }

    @Override
    public boolean test(R record) {
        LOGGER.debug("Evaluating predicate");
        return false;
    }

    @Override
    public void close() {
        LOGGER.debug("Closing predicate");
        // Close any resource you might have open
    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOGGER.debug("Configuring predicate");
        config = new truePredicateConfig(configs);
    }
}
