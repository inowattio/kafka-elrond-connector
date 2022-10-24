package org.example.testtools;

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

import org.example.testtools.client.ConnectClient;
import org.example.testtools.providers.BrokerProvider;
import org.example.testtools.providers.ConnectProvider;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectTestCluster implements BrokerProvider, ConnectProvider, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectTestCluster.class);

    private static final String CONNECT_NAME_PREFIX = "connect-test-";
    private static final String INTERNAL_TOPIC_PREFIX = "_";
    private static final String CONFIG_TOPIC_POSTFIX = "-config";
    private static final String OFFSET_TOPIC_POSTFIX = "-offset";
    private static final String STATUS_TOPIC_POSTFIX = "-status";
    private static final AtomicInteger CONNECT_ID_COUNTER = new AtomicInteger(1);

    private final KafkaTestCluster kafka;
    private final String connectName;
    private final String connectConfigTopicName;
    private final String connectOffsetTopicName;
    private final String connectStatusTopicName;
    private final String listener;
    private Connect connect;
    private ConnectClient connectClient;

    public ConnectTestCluster(int zookeeperPort, int brokerPort, int connectPort) {
        connectName = CONNECT_NAME_PREFIX + CONNECT_ID_COUNTER.getAndIncrement();
        LOG.info("Creating ConnectTestCluster {}", connectName);
        kafka = new KafkaTestCluster(zookeeperPort, brokerPort);
        connectConfigTopicName = INTERNAL_TOPIC_PREFIX + connectName + CONFIG_TOPIC_POSTFIX;
        connectOffsetTopicName = INTERNAL_TOPIC_PREFIX + connectName + OFFSET_TOPIC_POSTFIX;
        connectStatusTopicName = INTERNAL_TOPIC_PREFIX + connectName + STATUS_TOPIC_POSTFIX;
        listener = String.format("http://localhost:%d", connectPort);
    }

    public void start() {
        LOG.info("Starting ConnectTestCluster {}", connectName);
        kafka.start();
        createTopic(connectConfigTopicName, 1, true);
        createTopic(connectOffsetTopicName, 1, true);
        createTopic(connectStatusTopicName, 1, true);

        connect = new ConnectDistributed().startConnect(workerProperties());
        connectClient = ConnectClient.createClient(connect.restUrl().toString());

        LOG.info("ConnectTestCluster {} Started", connectName);
    }

    public void stop() {
        LOG.info("Stopping ConnectTestCluster {}", connectName);
        if (connect != null) {
            LOG.info("Stopping Connect {}", connectName);
            connect.stop();
            connect.awaitStop();
            connect = null;
            LOG.info("Connect {} Stopped", connectName);
        }

        if (kafka != null) {
            deleteTopic(connectConfigTopicName);
            deleteTopic(connectOffsetTopicName);
            deleteTopic(connectStatusTopicName);
            kafka.stop();
        }
        LOG.info("ConnectTestCluster {} Stopped", connectName);
    }

    @Override
    public void close() {
        LOG.info("Closing ConnectTestCluster {}", connectName);
        stop();
        LOG.info("ConnectTestCluster {} Closed", connectName);
    }

    @Override
    public ConnectClient connectClient() {
        return connectClient;
    }

    private Map<String, String> workerProperties() {
        final Map<String, String> workerProperties = new HashMap<>();
        kafka.clientConnectionProperties().entrySet().stream()
                .filter(entry -> entry.getValue() instanceof String)
                .forEach(entry -> workerProperties.put(entry.getKey(), (String) entry.getValue()));
        workerProperties.put(DistributedConfig.LISTENERS_CONFIG, listener);
        workerProperties.put(DistributedConfig.GROUP_ID_CONFIG, connectName);
        workerProperties.put(DistributedConfig.CONFIG_TOPIC_CONFIG, connectConfigTopicName);
        workerProperties.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProperties.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, connectOffsetTopicName);
        workerProperties.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProperties.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, connectStatusTopicName);
        workerProperties.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProperties.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
        workerProperties.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());

        return workerProperties;
    }

    @Override
    public void createTopic(String name, int partitions, boolean compacted) {
        kafka.createTopic(name, partitions, compacted);
    }

    @Override
    public void deleteTopic(String name) {
        kafka.deleteTopic(name);
    }

    @Override
    public String bootstrapServers() {
        return kafka.bootstrapServers();
    }

    @Override
    public Map<String, Object> clientConnectionProperties() {
        return kafka.clientConnectionProperties();
    }

    @Override
    public Map<String, Object> consumerProperties(String groupId) {
        return kafka.consumerProperties(groupId);
    }

    @Override
    public ConsumerRecords<byte[], byte[]> consume(int maxRecords, long maxDuration, String groupId, Collection<String> topics) {
        return kafka.consume(maxRecords, maxDuration, groupId, topics);
    }

    @Override
    public Map<String, Object> producerProperties(String transactionalId) {
        return kafka.producerProperties(transactionalId);
    }

    @Override
    public long produce(ProducerRecord<byte[], byte[]> record) {
        return kafka.produce(record);
    }
}
