/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeKafkaContainerTest {

    private NativeKafkaContainer kafkaContainer;

    @BeforeEach
    void setUp() {
        kafkaContainer = new NativeKafkaContainer();
    }

    @Test
    void testDefaultInitialization() {
        assertThat(kafkaContainer, is(notNullValue()));
        assertThat(kafkaContainer.getExposedPorts(), hasItem(NativeKafkaContainer.KAFKA_PORT));
        assertThat(kafkaContainer.getNodeId(), is(1));
    }

    @Test
    void testCustomDockerImageName() {
        NativeKafkaContainer customContainer = new NativeKafkaContainer("apache/kafka-native:3.8.1");
        assertThat(customContainer, is(notNullValue()));
    }

    @Test
    void testWithNodeId() {
        kafkaContainer.withNodeId(5);
        assertThat(kafkaContainer.getNodeId(), is(5));
    }

    @Test
    void testWithPortNegativeThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> kafkaContainer.withPort(-1));
    }

    @Test
    void testWithPortZeroThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> kafkaContainer.withPort(0));
    }

    @Test
    void testWithLogCollection() {
        kafkaContainer.withLogCollection();
        assertThat(kafkaContainer, is(notNullValue()));
    }

    @Test
    void testWithLogCollectionCustomPath() {
        kafkaContainer.withLogCollection("target/logs/custom.log");
        assertThat(kafkaContainer, is(notNullValue()));
    }

    @Test
    void testWithLogCollectionEmptyPathThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> kafkaContainer.withLogCollection(""));
    }

    @Test
    void testWithLogCollectionNullPathThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> kafkaContainer.withLogCollection(null));
    }

    @Test
    void testBootstrapServersFormat() {
        assertThat(kafkaContainer.getBootstrapServers(), startsWith("PLAINTEXT://"));
    }

    @Test
    void testBootstrapControllersFormat() {
        assertThat(kafkaContainer.getBootstrapControllers(), startsWith("CONTROLLER://"));
    }

    @Test
    void testNetworkBootstrapServersFormat() {
        String networkBootstrap = kafkaContainer.getNetworkBootstrapServers();
        assertThat(networkBootstrap, containsString("broker-"));
        assertThat(networkBootstrap, containsString(":" + NativeKafkaContainer.KAFKA_PORT));
    }

    @Test
    void testNetworkBootstrapControllersFormat() {
        String networkControllers = kafkaContainer.getNetworkBootstrapControllers();
        assertThat(networkControllers, containsString("broker-"));
        assertThat(networkControllers, containsString(":" + NativeKafkaContainer.CONTROLLER_PORT));
    }
}