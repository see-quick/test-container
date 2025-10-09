/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Integration tests for StrimziKafkaCluster using Apache Kafka native images.
 * This test suite validates that the cluster can be started with native images
 * and perform basic Kafka operations.
 */
public class StrimziKafkaClusterNativeIT {

    private StrimziKafkaCluster systemUnderTest;

    @Test
    void testSingleNodeNativeCluster() throws ExecutionException, InterruptedException, TimeoutException {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withNativeImage()
            .build();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), notNullValue());
        assertThat(systemUnderTest.getBootstrapServers(), startsWith("PLAINTEXT://"));

        // Verify basic Kafka operations
        verifyKafkaOperations();
    }

    @Test
    void testSingleNodeNativeClusterWithSpecificVersion() throws ExecutionException, InterruptedException, TimeoutException {
        // Using a specific Kafka native version
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withNativeImage()
            .withKafkaVersion("latest")
            .build();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), notNullValue());

        // Verify basic Kafka operations
        verifyKafkaOperations();
    }

    @Test
    void testProduceAndConsumeMessagesOnNativeCluster() throws ExecutionException, InterruptedException, TimeoutException {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withNativeImage()
            .build();

        systemUnderTest.start();

        final String topicName = "native-test-topic";

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());
        consumerProperties.put("group.id", "native-test-group");
        consumerProperties.put("auto.offset.reset", "earliest");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer());
             final AdminClient adminClient = AdminClient.create(ImmutableMap.of(
                 AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()))) {

            final Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            // Produce messages
            producer.send(new ProducerRecord<>(topicName, "key1", "native-message-1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "native-message-2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "native-message-3")).get();

            TopicPartition partition = new TopicPartition(topicName, 0);
            consumer.assign(Collections.singleton(partition));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

            assertThat(records.count(), equalTo(3));
            assertThat(records.records(partition).get(0).value(), equalTo("native-message-1"));
            assertThat(records.records(partition).get(1).value(), equalTo("native-message-2"));
            assertThat(records.records(partition).get(2).value(), equalTo("native-message-3"));
        }
    }

    @Test
    void testNetworkBootstrapServers() {
        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withNativeImage()
            .build();

        systemUnderTest.start();

        // Test network bootstrap servers (for container-to-container communication)
        String networkBootstrap = systemUnderTest.getNetworkBootstrapServers();
        assertThat(networkBootstrap, notNullValue());
        assertThat(networkBootstrap, containsString("broker-0"));

        // Test network bootstrap controllers
        String networkControllers = systemUnderTest.getNetworkBootstrapControllers();
        assertThat(networkControllers, notNullValue());
        assertThat(networkControllers, containsString("broker-0"));
    }

    private void verifyKafkaOperations() throws InterruptedException, ExecutionException, TimeoutException {
        final String topicName = "verification-topic";

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());
        consumerProperties.put("group.id", "verification-group");
        consumerProperties.put("auto.offset.reset", "earliest");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer());
             final AdminClient adminClient = AdminClient.create(ImmutableMap.of(
                 AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()))) {

            final Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            producer.send(new ProducerRecord<>(topicName, "key", "value")).get();

            TopicPartition partition = new TopicPartition(topicName, 0);
            consumer.assign(Collections.singleton(partition));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            assertThat(records.count(), equalTo(1));
            assertThat(records.records(partition).get(0).value(), equalTo("value"));
        }
    }

    @AfterEach
    void tearDown() {
        if (systemUnderTest != null) {
            systemUnderTest.stop();
        }
    }
}