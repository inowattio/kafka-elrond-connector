package org.example.testtools.jupiter;

/*-
 * ========================LICENSE_START=================================
 * Kafka Synchronisation Connectors for Kafka Connect
 * %%
 * Copyright (C) 2021 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.example.testtools.KafkaTestCluster;
import org.example.testtools.PortAllocator;
import org.example.testtools.providers.BrokerProvider;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaExtension implements BrokerProvider, BeforeEachCallback, AfterEachCallback, ExtensionContext.Store.CloseableResource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaExtension.class);
    private final KafkaTestCluster testCluster;
    private final List<Integer> allocatedPorts = new ArrayList<>();
    private String currentTest = null;
    private Optional<Logger> currentLogger = Optional.empty();

    public KafkaExtension() {
        allocatedPorts.addAll(PortAllocator.findAndAllocatePorts(2));
        testCluster = new KafkaTestCluster(allocatedPorts.get(0), allocatedPorts.get(1));
    }

    public KafkaExtension(int zookeeperPort, int brokerPort) {
        testCluster = new KafkaTestCluster(zookeeperPort, brokerPort);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        currentLogger = Optional.ofNullable(LoggerFactory.getLogger(context.getRequiredTestClass()));
        currentTest = context.getDisplayName();
        currentLogger.orElse(LOG).info("Starting Connect test server for {}", currentTest);
        testCluster.start();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        currentLogger = Optional.ofNullable(LoggerFactory.getLogger(context.getRequiredTestClass()));
        currentTest = context.getDisplayName();
        currentLogger.orElse(LOG).info("Starting Connect test server for {}", currentTest);
        testCluster.stop();
    }

    @Override
    public void createTopic(String name, int partitions, boolean compacted) {
        testCluster.createTopic(name, partitions, compacted);
    }

    @Override
    public void deleteTopic(String name) {
        testCluster.deleteTopic(name);
    }

    @Override
    public String bootstrapServers() {
        return testCluster.bootstrapServers();
    }

    @Override
    public Map<String, Object> clientConnectionProperties() {
        return testCluster.clientConnectionProperties();
    }

    @Override
    public Map<String, Object> consumerProperties(String groupId) {
        return testCluster.consumerProperties(groupId);
    }

    @Override
    public ConsumerRecords<byte[], byte[]> consume(int maxRecords, long maxDuration, String groupId, Collection<String> topics) {
        return testCluster.consume(maxRecords, maxDuration, groupId, topics);
    }

    @Override
    public Map<String, Object> producerProperties(String transactionalId) {
        return testCluster.producerProperties(transactionalId);
    }

    @Override
    public long produce(ProducerRecord<byte[], byte[]> record) {
        return testCluster.produce(record);
    }

    @Override
    public void close() {
        testCluster.close();
        PortAllocator.deallocatePorts(allocatedPorts);
        allocatedPorts.clear();
    }
}
