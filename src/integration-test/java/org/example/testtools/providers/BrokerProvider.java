package org.example.testtools.providers;

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

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * A wrapper class to easily implement all providers
 */
public interface BrokerProvider {
    default int getDefaultTopicPartitions() {
        return 1;
    }

    default void createTopic(String name) {
        createTopic(name, getDefaultTopicPartitions());
    }

    default void createTopic(String name, int partitions) {
        createTopic(name, partitions, false);
    }

    void createTopic(String name, int partitions, boolean compacted);

    void deleteTopic(String name);

    default Map<String, Object> adminProperties() {
        return clientConnectionProperties();
    }

    String bootstrapServers();

    Map<String, Object> clientConnectionProperties();

    default Map<String, Object> consumerProperties() {
        return consumerProperties(null);
    }

    Map<String, Object> consumerProperties(String groupId);

    default long produce(String topic, byte[] key, byte[] value) {
        return produce(new ProducerRecord<>(topic, null, key, value, null));
    }

    default long produce(String topic, Integer partition, byte[] key, byte[] value) {
        return produce(new ProducerRecord<>(topic, partition, key, value, null));
    }

    default long produce(String topic, Integer partition, byte[] key, byte[] value, RecordHeaders headers) {
        return produce(new ProducerRecord<>(topic, partition, key, value, headers));
    }

    long produce(ProducerRecord<byte[], byte[]> record);

    /**
     * Generate a unique transactional id to use in producers.
     *
     * @return the new unique transactional id
     */
    default String generateTransactionalId() {
        return UUID.randomUUID().toString();
    }


    default Map<String, Object> producerProperties() {
        return producerProperties(null);
    }

    Map<String, Object> producerProperties(String transactionalId);

    /**
     * Consume at least n records in a given duration. The duration starts after consumer subscription is completed
     *
     * @param maxRecords  the number of expected records in this topic.
     * @param maxDuration the max duration to wait for these records (in milliseconds).
     * @param topic       the topic to subscribe and consume records from.
     * @return a {@link ConsumerRecords} collection containing at least maxRecords records.
     */
    default ConsumerRecords<byte[], byte[]> consume(int maxRecords, long maxDuration, String topic) {
        return consume(maxRecords, maxDuration, null, Collections.singleton(topic));
    }

    /**
     * Consume at least n records in a given duration. The duration starts after consumer subscription is completed
     *
     * @param maxRecords  the number of expected records in this topic.
     * @param maxDuration the max duration to wait for these records (in milliseconds).
     * @param groupId     the group id to use in this consumer
     * @param topic       the topic to subscribe and consume records from.
     * @return a {@link ConsumerRecords} collection containing at least maxRecords records.
     */
    default ConsumerRecords<byte[], byte[]> consume(int maxRecords, long maxDuration, String groupId, String topic) {
        return consume(maxRecords, maxDuration, groupId, Collections.singleton(topic));
    }

    /**
     * Consume at least n records in a given duration. The duration starts after consumer subscription is completed
     *
     * @param maxRecords  the number of expected records in this topic.
     * @param maxDuration the max duration to wait for these records (in milliseconds).
     * @param topics      the topics to subscribe and consume records from.
     * @return a {@link ConsumerRecords} collection containing at least maxRecords records.
     */
    default ConsumerRecords<byte[], byte[]> consume(int maxRecords, long maxDuration, Collection<String> topics) {
        return consume(maxRecords, maxDuration, null, topics);
    }

    /**
     * Consume at least n records in a given duration. The duration starts after consumer subscription is completed
     *
     * @param maxRecords  the number of expected records in this topic.
     * @param maxDuration the max duration to wait for these records (in milliseconds).
     * @param groupId     The optional groupId to use.
     * @param topics      the topics to subscribe and consume records from.
     * @return a {@link ConsumerRecords} collection containing at least maxRecords records.
     */
    ConsumerRecords<byte[], byte[]> consume(int maxRecords, long maxDuration, String groupId, Collection<String> topics);

    /**
     * Generate a unique group id to use in consumers.
     *
     * @return the new unique group id
     */
    default String generateGroupId() {
        return UUID.randomUUID().toString();
    }

}
