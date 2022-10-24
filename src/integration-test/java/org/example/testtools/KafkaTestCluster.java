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

import org.example.testtools.providers.BrokerProvider;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

public class KafkaTestCluster implements BrokerProvider, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestCluster.class);
    private static final long DEFAULT_FUTURE_GET_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    private static final short REPLICATION_FACTOR = 1;
    private static final AtomicInteger BROKER_ID_COUNTER = new AtomicInteger(1);
    private final int brokerPort;
    private final int zookeeperPort;
    private final int clusterId;
    private TestingServer zookeeper;
    private KafkaServer broker;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private File logDir;
    private KafkaProducer<byte[], byte[]> internalProducer;

    public KafkaTestCluster(int zookeeperPort, int brokerPort) {
        clusterId = BROKER_ID_COUNTER.getAndIncrement();
        LOG.info("Creating KafkaTestCluster {}", clusterId);
        this.zookeeperPort = zookeeperPort;
        this.brokerPort = brokerPort;
        zookeeper = null;
        broker = null;
        internalProducer = null;
    }

    public void start() {
        try {
            LOG.info("Starting KafkaTestCluster {}", clusterId);
            // Start zookeeper
            System.setProperty("zookeeper.serverCnxnFactory",
                    "org.apache.zookeeper.server.NettyServerCnxnFactory");
            LOG.info("Starting ZooKeeper on port {}", zookeeperPort);
            InstanceSpec spec = new InstanceSpec(null, zookeeperPort, -1, -1, true, -1, 2000, 10);
            zookeeper = new TestingServer(spec, true);

            // Start broker
            LOG.info("Starting Kafka {} on port {}", clusterId, brokerPort);
            try {
                logDir = Files.createTempDirectory("kafka").toFile();
            } catch (IOException e) {
                throw new RuntimeException("Unable to start Kafka", e);
            }

            broker = new KafkaServer(new KafkaConfig(brokerProperties()), Time.SYSTEM, Option.apply("IT_Cluster"), false);
            broker.startup();

            internalProducer = new KafkaProducer<>(producerProperties());

            isRunning.set(true);
            LOG.info("KafkaTestCluster {} Started", clusterId);
        } catch (Exception e) {
            throw new RuntimeException("Unable to start KafkaTestCluster", e);
        }
    }

    public void stop() {
        try {
            LOG.info("Stopping KafkaTestCluster {}", clusterId);
            if (internalProducer != null) {
                internalProducer.close();
                internalProducer = null;
            }

            if (broker != null) {
                LOG.info("Shutting down Kafka for KafkaTestCluster {} on port {}", clusterId, brokerPort);
                broker.shutdown();
                broker.awaitShutdown();
                try {
                    FileUtils.cleanDirectory(logDir);
                } catch (IOException e) {
                    LOG.warn("Could not clean the directory {} for KafkaTestCluster {}", logDir.getAbsolutePath(), clusterId);
                }
                try {
                    Files.delete(logDir.toPath());
                } catch (IOException e) {
                    LOG.error("Error deleting the directory {} for KafkaTestCluster {}", logDir.getAbsolutePath(), clusterId);
                }
                broker = null;
            }
            if (zookeeper != null) {
                zookeeper.stop();
                zookeeper.close();
                zookeeper = null;
            }
            isRunning.set(false);
            LOG.info("KafkaTestCluster {} Stopped", clusterId);
        } catch (Exception e) {
            throw new RuntimeException("Unable to stop KafkaTestCluster " + clusterId, e);
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void close() {
        LOG.info("Closing KafkaTestCluster {}", clusterId);
        stop();
        LOG.info("KafkaTestCluster Stopped {}", clusterId);
    }

    private Map<String, Object> brokerProperties() {
        Map<String, Object> brokerProperties = new HashMap<>();
        brokerProperties.put("zookeeper.connect", zookeeper.getConnectString());
        brokerProperties.put("broker.id", Integer.toString(clusterId));
        brokerProperties.put("log.dir", logDir.getAbsolutePath());
        brokerProperties.put("log.flush.interval.messages", String.valueOf(1));
        brokerProperties.put("offsets.topic.replication.factor", "1");
        brokerProperties.put("min.insync.replicas", "1");
        brokerProperties.put("transaction.state.log.min.isr", "1");
        brokerProperties.put("transaction.state.log.replication.factor", "1");
        brokerProperties.put("listeners", format("PLAINTEXT://localhost:%d", brokerPort));
        brokerProperties.put("advertised.listeners", format("PLAINTEXT://localhost:%d", brokerPort));
        brokerProperties.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
        brokerProperties.put("allow.everyone.if.no.acl.found", "true");
        brokerProperties.put("auto.create.topics.enable", "false");
        brokerProperties.put("log.segment.delete.delay.ms", "100");
        brokerProperties.put("delete.topic.enable", "true");

        return brokerProperties;
    }

    @Override
    public String bootstrapServers() {
        return format("localhost:%d", brokerPort);
    }

    @Override
    public Map<String, Object> clientConnectionProperties() {
        Map<String, Object> connectionProperties = new HashMap<>();
        connectionProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        connectionProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        return connectionProperties;
    }

    @Override
    public Map<String, Object> producerProperties(String transactionalId) {
        Map<String, Object> producerProperties = clientConnectionProperties();
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        if (transactionalId != null) {
            producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }

        return producerProperties;
    }

    @Override
    public Map<String, Object> consumerProperties(String groupId) {
        Map<String, Object> consumerProperties = clientConnectionProperties();
        consumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        if (groupId != null) {
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        return consumerProperties;
    }

    @Override
    public void createTopic(String name, int partitions, boolean compacted) {
        LOG.info("Creating topic {} with {} partitions for KafkaTestCluster {}", name, partitions, clusterId);
        if (name == null) {
            throw new IllegalArgumentException("Topic name cannot be null");
        }

        try (AdminClient adminClient = AdminClient.create(adminProperties())) {
            NewTopic newTopic = new NewTopic(name, partitions, REPLICATION_FACTOR);
            newTopic.configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, compacted ? TopicConfig.CLEANUP_POLICY_COMPACT : TopicConfig.CLEANUP_POLICY_DELETE));

            CreateTopicsResult createResult = adminClient.createTopics(Collections.singleton(newTopic));
            try {
                createResult.all().get();
                LOG.info("Created topic {} with {} partitions for KafkaTestCluster {}", name, partitions, clusterId);
            } catch (InterruptedException e) {
                throw new RuntimeException("Create topic for topic " + name + " was interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TopicExistsException) {
                    LOG.info("Topic {} already exists", name);
                    return;
                }
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException("Create topic for topic " + name + " has failed", cause == null ? e : cause);
            }
        }
    }

    @Override
    public void deleteTopic(String name) {
        LOG.info("Deleting topic {} for KafkaTestCluster {}", name, clusterId);
        if (name == null) {
            throw new IllegalArgumentException("Topic name cannot be null");
        }

        try (AdminClient adminClient = AdminClient.create(adminProperties())) {
            DeleteTopicsResult deleteResult = adminClient.deleteTopics(Collections.singleton(name));
            deleteResult.values().get(name).get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Delete topic " + name + " was interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Delete topic " + name + " has failed", e.getCause() == null ? e : e.getCause());
        }
    }

    @Override
    public ConsumerRecords<byte[], byte[]> consume(int maxRecords, long maxDuration, String groupId, Collection<String> topics) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        int consumedRecords = 0;
        Map<String, Object> consumerProperties = consumerProperties(groupId);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxRecords);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            if (groupId == null) {
                consumer.assign(topics.stream()
                        .map(consumer::partitionsFor)
                        .flatMap(Collection::stream)
                        .map(ti -> new TopicPartition(ti.topic(), ti.partition()))
                        .collect(Collectors.toList())
                );
            } else {
                consumer.subscribe(topics);
            }
            final long startMillis = System.currentTimeMillis();
            long durationLeft = maxDuration;
            while (durationLeft > 0 && consumedRecords < maxRecords) {
                LOG.info("Consuming from {} for {} millis from KafkaTestCluster {}", String.join(",", topics), durationLeft, clusterId);
                ConsumerRecords<byte[], byte[]> rec = consumer.poll(Duration.ofMillis(durationLeft));
                if (rec.isEmpty()) {
                    durationLeft = maxDuration - (System.currentTimeMillis() - startMillis);
                    continue;
                }
                for (TopicPartition partition : rec.partitions()) {
                    final List<ConsumerRecord<byte[], byte[]>> r = rec.records(partition);
                    records.computeIfAbsent(partition, t -> new ArrayList<>()).addAll(r);
                    consumedRecords += r.size();
                }
                durationLeft = maxDuration - (System.currentTimeMillis() - startMillis);
            }
        }
        return new ConsumerRecords<>(records);
    }

    @Override
    public long produce(ProducerRecord<byte[], byte[]> record) {
        try {
            return internalProducer.send(record).get(DEFAULT_FUTURE_GET_TIMEOUT, TimeUnit.MILLISECONDS).offset();
        } catch (Exception e) {
            throw new KafkaException("Could not produce message: " + record, e);
        }
    }

}
