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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Integration tests for NativeKafkaContainer using Apache Kafka's native image.
 */
public class NativeKafkaContainerIT {

    private NativeKafkaContainer systemUnderTest;

    @Test
    void testStartContainerWithDefaultConfiguration() throws ExecutionException, InterruptedException, TimeoutException {
        systemUnderTest = new NativeKafkaContainer()
            .withNodeId(1)
            .withPort(9092)
            .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.getBootstrapServers(), notNullValue());
        assertThat(systemUnderTest.getBootstrapServers(), startsWith("PLAINTEXT://"));

        String logsFromKafka = systemUnderTest.getLogs();
        assertThat(logsFromKafka, containsString("Kafka Server started"));

        verify();
    }

    @Test
    void testStartContainerWithCustomEnvironmentVariables() {
        Map<String, String> envVars = new HashMap<>();
        envVars.put("KAFKA_NUM_PARTITIONS", "5");
        envVars.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

        systemUnderTest = new NativeKafkaContainer()
            .withNodeId(1)
            .withPort(9092)
            .withEnvironmentVariables(envVars)
            .waitForRunning();

        systemUnderTest.start();

        assertThat(systemUnderTest.isRunning(), is(true));
        assertThat(systemUnderTest.getBootstrapServers(), notNullValue());
    }

    @Test
    void testProduceAndConsumeMessages() throws ExecutionException, InterruptedException, TimeoutException {
        systemUnderTest = new NativeKafkaContainer()
            .withNodeId(1)
            .withPort(9092)
            .waitForRunning();

        systemUnderTest.start();

        final String topicName = "test-topic";

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", systemUnderTest.getBootstrapServers());
        consumerProperties.put("group.id", "test-group");
        consumerProperties.put("auto.offset.reset", "earliest");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer());
             final AdminClient adminClient = AdminClient.create(ImmutableMap.of(
                 AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()))) {

            final Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            producer.send(new ProducerRecord<>(topicName, "key1", "message1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "message2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "message3")).get();

            TopicPartition partition = new TopicPartition(topicName, 0);
            consumer.assign(Collections.singleton(partition));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

            assertThat(records.count(), equalTo(3));
            assertThat(records.records(partition).get(0).value(), equalTo("message1"));
            assertThat(records.records(partition).get(1).value(), equalTo("message2"));
            assertThat(records.records(partition).get(2).value(), equalTo("message3"));
        }
    }

    @Test
    void testLogCollectionWithDefaultPath() throws IOException {
        String expectedLogFilePath = "target/strimzi-test-container-logs/kafka-native-container-1.log";
        Path logPath = Paths.get(expectedLogFilePath);

        // Clean up any existing log file
        Files.deleteIfExists(logPath);

        systemUnderTest = new NativeKafkaContainer()
            .withNodeId(1)
            .withPort(9092)
            .withLogCollection()
            .waitForRunning();

        systemUnderTest.start();

        // Verify the container is running and has logs
        assertThat(systemUnderTest.getLogs(), containsString("Kafka Server started"));

        systemUnderTest.stop();

        // Verify log file was created
        assertThat("Log file should exist", Files.exists(logPath), is(true));

        // Verify log file contains expected content
        String logContent = Files.readString(logPath);
        assertThat(logContent, containsString("Kafka Server started"));

        // Clean up
        Files.deleteIfExists(logPath);
    }

    @Test
    void testLogCollectionWithCustomPath() throws IOException {
        String customLogPath = "test-logs/native-kafka.log";
        Path logPath = Paths.get(customLogPath);

        // Clean up any existing log file and directory
        Files.deleteIfExists(logPath);

        systemUnderTest = new NativeKafkaContainer()
            .withPort(9092)
            .withNodeId(1)
            .withLogCollection(customLogPath)
            .waitForRunning();

        systemUnderTest.start();

        // Verify the container is running
        assertThat(systemUnderTest.getLogs(), containsString("Kafka Server started"));

        systemUnderTest.stop();

        // Verify log file was created at custom location
        assertThat("Log file should exist at custom path", Files.exists(logPath), is(true));

        // Verify log file contains expected content
        String logContent = Files.readString(logPath);
        assertThat(logContent, containsString("Kafka Server started"));

        // Clean up
        Files.deleteIfExists(logPath);
        if (logPath.getParent() != null) {
            Files.deleteIfExists(logPath.getParent());
        }
    }

    @Test
    void testBootstrapServersAndControllers() {
        systemUnderTest = new NativeKafkaContainer()
            .withNodeId(1)
            .withPort(9092)
            .waitForRunning();

        systemUnderTest.start();

        // Test bootstrap servers
        String bootstrapServers = systemUnderTest.getBootstrapServers();
        assertThat(bootstrapServers, notNullValue());
        assertThat(bootstrapServers, startsWith("PLAINTEXT://"));

        // Test bootstrap controllers
        String bootstrapControllers = systemUnderTest.getBootstrapControllers();
        assertThat(bootstrapControllers, notNullValue());
        assertThat(bootstrapControllers, startsWith("CONTROLLER://"));

        // Test network bootstrap servers
        String networkBootstrap = systemUnderTest.getNetworkBootstrapServers();
        assertThat(networkBootstrap, notNullValue());
        assertThat(networkBootstrap, containsString("broker-1"));

        // Test network bootstrap controllers
        String networkControllers = systemUnderTest.getNetworkBootstrapControllers();
        assertThat(networkControllers, notNullValue());
        assertThat(networkControllers, containsString("broker-1"));
    }

    private void verify() throws InterruptedException, ExecutionException, TimeoutException {
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