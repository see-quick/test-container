/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * NativeKafkaContainer is a single-node instance of Kafka using the native image from apache/kafka-native.
 * This container runs Kafka in KRaft mode (without ZooKeeper) using environment variables for configuration,
 * which is optimized for faster startup times using GraalVM native image.
 * <p>
 * This implementation uses the official Apache Kafka native image with environment-based configuration
 * as shown in the Apache Kafka documentation.
 * </p>
 */
public class NativeKafkaContainer extends GenericContainer<NativeKafkaContainer> implements KafkaContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeKafkaContainer.class);

    /**
     * Default Kafka port
     */
    public static final int KAFKA_PORT = 9092;

    /**
     * Default Kafka controller port
     */
    public static final int CONTROLLER_PORT = 9093;

    /**
     * Default Docker image for Apache Kafka native
     */
    public static final String DEFAULT_IMAGE_NAME = "apache/kafka-native:latest";

    /**
     * Network alias prefix for container discovery
     */
    protected static final String NETWORK_ALIAS_PREFIX = "broker-";

    private final boolean enableBrokerContainerSlf4jLogging = Boolean.parseBoolean(
        System.getenv().getOrDefault("STRIMZI_TEST_CONTAINER_LOGGING_ENABLED", "false"));

    // Instance attributes
    private int kafkaExposedPort;
    private int controllerExposedPort;
    private int nodeId = 1;
    private Map<String, String> environmentOverrides = new HashMap<>();
    private String logFilePath;

    /**
     * Creates a new NativeKafkaContainer with the default Apache Kafka native image.
     */
    public NativeKafkaContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    /**
     * Creates a new NativeKafkaContainer with a specific Docker image.
     *
     * @param dockerImageName the Docker image name to use
     */
    public NativeKafkaContainer(String dockerImageName) {
        super(dockerImageName);
        super.setNetwork(Network.SHARED);
        // Expose both Kafka and controller ports for combined-role node
        super.setExposedPorts(Arrays.asList(KAFKA_PORT, CONTROLLER_PORT));
    }

    @Override
    @DoNotMutate
    protected void doStart() {
        // Setup network alias
        super.withNetworkAliases(NETWORK_ALIAS_PREFIX + this.nodeId);

        // Configure default environment variables
        configureDefaultEnvironment();

        // Apply any user-provided overrides
        this.environmentOverrides.forEach(super::addEnv);

        // Setup logging
        if (this.enableBrokerContainerSlf4jLogging) {
            this.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("NativeKafkaContainer-" + this.nodeId)));
        }

        super.doStart();
    }

    @DoNotMutate
    private void configureDefaultEnvironment() {
        // Core Kafka settings - matching the original docker run command
        addEnv("KAFKA_NODE_ID", String.valueOf(this.nodeId));
        addEnv("KAFKA_PROCESS_ROLES", "broker,controller");
        addEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",CONTROLLER://0.0.0.0:" + CONTROLLER_PORT);

        // CRITICAL: Advertise as localhost so clients from the host can connect via port mapping
        // The container listens on 0.0.0.0:9092, Docker maps it to localhost:<random-port>
        // When Kafka advertises "localhost:9092", clients resolve it back to localhost (the host machine)
        // and Docker's port mapping (localhost:<random-port> -> container:9092) makes it work
        addEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:" + KAFKA_PORT);

        addEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        addEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        addEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", this.nodeId + "@localhost:" + CONTROLLER_PORT);

        // Replication settings for single-node setup
        addEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        addEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        addEnv("KAFKA_NUM_PARTITIONS", "3");
    }

    @Override
    @DoNotMutate
    protected void containerIsStarting(final InspectContainerResponse containerInfo) {
        super.containerIsStarting(containerInfo);

        this.kafkaExposedPort = getMappedPort(KAFKA_PORT);
        this.controllerExposedPort = getMappedPort(CONTROLLER_PORT);

        LOGGER.info("Mapped Kafka port: {}", kafkaExposedPort);
        LOGGER.info("Mapped controller port: {}", controllerExposedPort);
    }

    @Override
    @DoNotMutate
    public void stop() {
        // Collect logs if log collection is enabled
        if (this.logFilePath != null) {
            collectLogs();
        }
        super.stop();
    }

    @DoNotMutate
    private void collectLogs() {
        try {
            String finalLogPath = this.logFilePath;

            if (this.logFilePath.endsWith("/")) {
                String fileName = "kafka-native-container-" + this.nodeId + ".log";
                finalLogPath = this.logFilePath + fileName;
            }

            LOGGER.info("Collecting logs to file: {}", finalLogPath);
            final String logs = getLogs();
            final Path logPath = Paths.get(finalLogPath);

            // Create directories if they don't exist
            if (logPath.getParent() != null) {
                Files.createDirectories(logPath.getParent());
            }

            Files.writeString(logPath, logs, StandardCharsets.UTF_8);

            LOGGER.info("Successfully collected logs to: {}", logPath.toAbsolutePath());
        } catch (IOException e) {
            LOGGER.error("Failed to collect logs to file: {}", this.logFilePath, e);
            throw new RuntimeException("Failed to collect logs to file: " + this.logFilePath, e);
        }
    }

    /**
     * Fluent method to set a waiting strategy to wait until the broker is ready.
     *
     * @return NativeKafkaContainer instance
     */
    @DoNotMutate
    public NativeKafkaContainer waitForRunning() {
        super.waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1));
        return this;
    }

    @Override
    @DoNotMutate
    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getHost(), this.kafkaExposedPort);
    }

    /**
     * Get the bootstrap servers that containers on the same network should use to connect.
     *
     * @return Kafka bootstrap servers for container network
     */
    public String getNetworkBootstrapServers() {
        return NETWORK_ALIAS_PREFIX + nodeId + ":" + KAFKA_PORT;
    }

    @Override
    @DoNotMutate
    public String getBootstrapControllers() {
        return String.format("CONTROLLER://%s:%d", getHost(), this.controllerExposedPort);
    }

    /**
     * Get the bootstrap controllers that containers on the same network should use to connect to controllers.
     *
     * @return Kafka controller endpoints for container network
     */
    public String getNetworkBootstrapControllers() {
        return NETWORK_ALIAS_PREFIX + nodeId + ":" + CONTROLLER_PORT;
    }

    /**
     * Fluent method to set the node ID.
     *
     * @param nodeId the node ID
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withNodeId(int nodeId) {
        this.nodeId = nodeId;
        return self();
    }

    /**
     * Fluent method to set a fixed exposed port.
     *
     * @param fixedPort fixed port to expose
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withPort(int fixedPort) {
        if (fixedPort <= 0) {
            throw new IllegalArgumentException("The fixed Kafka port must be greater than 0");
        }
        addFixedExposedPort(fixedPort, KAFKA_PORT);
        return self();
    }

    /**
     * Fluent method to add or override environment variables.
     *
     * @param key   the environment variable name
     * @param value the environment variable value
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withEnvironmentVariable(String key, String value) {
        this.environmentOverrides.put(key, value);
        return self();
    }

    /**
     * Fluent method to add or override multiple environment variables.
     *
     * @param environmentMap map of environment variables
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withEnvironmentVariables(Map<String, String> environmentMap) {
        this.environmentOverrides.putAll(environmentMap);
        return self();
    }

    /**
     * Fluent method to enable log collection with a default log file path.
     *
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withLogCollection() {
        this.logFilePath = "target/strimzi-test-container-logs/";
        return self();
    }

    /**
     * Fluent method to enable log collection with a custom log file path.
     *
     * @param logFilePath the path where container logs will be saved
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withLogCollection(String logFilePath) {
        if (logFilePath != null && !logFilePath.trim().isEmpty()) {
            this.logFilePath = logFilePath.trim();
        } else {
            throw new IllegalArgumentException("Log file path cannot be null or empty.");
        }
        return self();
    }

    /**
     * Gets the node ID.
     *
     * @return the node ID
     */
    public int getNodeId() {
        return nodeId;
    }
}