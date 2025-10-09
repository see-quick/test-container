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
class NativeKafkaContainer extends GenericContainer<NativeKafkaContainer> implements KafkaContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeKafkaContainer.class);

    /**
     * Internal listener port (for inter-broker communication within Docker network)
     */
    public static final int INTERNAL_PORT = 9091;

    /**
     * Default Kafka port (external listener for clients outside Docker network)
     */
    public static final int KAFKA_PORT = 9092;

    /**
     * Default Kafka controller port
     */
    public static final int CONTROLLER_PORT = 9093;

    /**
     * Default Docker image for Apache Kafka native
     */
    public static final String DEFAULT_IMAGE_NAME = "apache/kafka-native";

    /**
     * Default Docker image tag for Apache Kafka native
     */
    public static final String DEFAULT_TAG = "latest";

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
    private Map<String, String> kafkaConfigurationMap;
    private String clusterId;
    private String logFilePath;

    /**
     * Creates a new NativeKafkaContainer with the default Apache Kafka native image.
     */
    public NativeKafkaContainer() {
        this(DEFAULT_IMAGE_NAME + ":" + DEFAULT_TAG);
    }

    /**
     * Creates a new NativeKafkaContainer with a specific Docker image.
     *
     * @param dockerImageName the Docker image name to use (can include tag, e.g., "apache/kafka-native:3.9.0")
     */
    public NativeKafkaContainer(String dockerImageName) {
        super(dockerImageName);
        super.setNetwork(Network.SHARED);
        // Expose Kafka (external), and controller ports for combined-role node
        // Internal port is only used within Docker network, Kafka port is for external clients
        super.setExposedPorts(Arrays.asList(KAFKA_PORT, CONTROLLER_PORT));
    }

    @Override
    @DoNotMutate
    protected void doStart() {
        // Setup network alias
        super.withNetworkAliases(NETWORK_ALIAS_PREFIX + this.nodeId);

        // Build default configuration, similar to StrimziKafkaContainer's buildDefaultServerProperties
        Map<String, String> defaultKafkaConfig = buildDefaultKafkaConfiguration();

        // Override defaults with user-provided configuration (if any)
        Map<String, String> finalKafkaConfig = overrideConfiguration(defaultKafkaConfig, this.kafkaConfigurationMap);

        // Convert Kafka configuration to environment variables
        Map<String, String> envVars = KafkaConfigMapper.toEnvironmentVariables(finalKafkaConfig);
        envVars.forEach(super::addEnv);

        // Add node ID - this is required for KRaft
        addEnv("KAFKA_NODE_ID", String.valueOf(this.nodeId));

        // Add cluster ID if provided (for multi-node clusters)
        if (this.clusterId != null) {
            addEnv("CLUSTER_ID", this.clusterId);
        }

        // Setup logging
        if (this.enableBrokerContainerSlf4jLogging) {
            this.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("NativeKafkaContainer-" + this.nodeId)));
        }

        super.doStart();
    }

    /**
     * Builds default Kafka configuration for standalone (single-node) usage.
     * This configuration is based on StrimziKafkaContainer's setCommonServerProperties and setKRaftProperties.
     * These defaults can be overridden by providing a kafkaConfigurationMap.
     *
     * @return Map of default Kafka configuration properties
     */
    @DoNotMutate
    private Map<String, String> buildDefaultKafkaConfiguration() {
        Map<String, String> config = new HashMap<>();

        // KRaft properties (from setKRaftProperties)
        config.put("process.roles", "broker,controller");
        config.put("controller.listener.names", "CONTROLLER");

        // Listener configuration - use dual listeners for cluster mode
        if (this.clusterId != null) {
            // Multi-node cluster configuration - use both internal and external listeners
            // INTERNAL (9091) for inter-broker communication, EXTERNAL (9092) for clients outside Docker network
            config.put("listeners",
                String.format("INTERNAL://0.0.0.0:%d,EXTERNAL://0.0.0.0:%d,CONTROLLER://0.0.0.0:%d",
                    INTERNAL_PORT, KAFKA_PORT, CONTROLLER_PORT));

            // Advertised listeners:
            // - INTERNAL uses network alias for inter-broker communication
            // - EXTERNAL uses localhost (TestContainers will map this to the host)
            // - CONTROLLER uses network alias for controller communication
            config.put("advertised.listeners",
                String.format("INTERNAL://%s%d:%d,EXTERNAL://localhost:%d,CONTROLLER://%s%d:%d",
                    NETWORK_ALIAS_PREFIX, this.nodeId, INTERNAL_PORT,
                    KAFKA_PORT,
                    NETWORK_ALIAS_PREFIX, this.nodeId, CONTROLLER_PORT));

            config.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT");
            config.put("inter.broker.listener.name", "INTERNAL");

            // For cluster mode, quorum voters will be provided by StrimziKafkaCluster
            // Don't set default quorum voters here as the cluster provides the full list
        } else {
            // Standalone single-node configuration - use localhost
            config.put("listeners", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",CONTROLLER://0.0.0.0:" + CONTROLLER_PORT);
            config.put("advertised.listeners", "PLAINTEXT://localhost:" + KAFKA_PORT);
            config.put("inter.broker.listener.name", "PLAINTEXT");
            config.put("listener.security.protocol.map", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");

            // For standalone containers, set default quorum voters
            config.put("controller.quorum.voters",
                String.format("%d@%s%d:%d", this.nodeId, NETWORK_ALIAS_PREFIX, this.nodeId, CONTROLLER_PORT));
        }

        // Common server properties (from setCommonServerProperties)
        config.put("num.network.threads", "3");
        config.put("num.io.threads", "8");
        config.put("socket.send.buffer.bytes", "102400");
        config.put("socket.receive.buffer.bytes", "102400");
        config.put("socket.request.max.bytes", "104857600");
        config.put("log.dirs", "/tmp/default-log-dir");
        config.put("num.partitions", "1");
        config.put("num.recovery.threads.per.data.dir", "1");
        config.put("offsets.topic.replication.factor", "1");
        config.put("transaction.state.log.replication.factor", "1");
        config.put("transaction.state.log.min.isr", "1");
        config.put("log.retention.hours", "168");
        config.put("log.retention.check.interval.ms", "300000");

        return config;
    }

    /**
     * Overrides default Kafka configuration with user-provided overrides.
     * Similar to StrimziKafkaContainer's overrideProperties method.
     *
     * @param defaultConfig The default Kafka configuration.
     * @param overrides     The properties to override. Can be null.
     * @return The combined configuration with overrides applied.
     */
    @DoNotMutate
    private Map<String, String> overrideConfiguration(Map<String, String> defaultConfig, Map<String, String> overrides) {
        Map<String, String> result = new HashMap<>(defaultConfig);

        // Apply overrides if provided
        if (overrides != null && !overrides.isEmpty()) {
            result.putAll(overrides);
        }

        return result;
    }

    @Override
    @DoNotMutate
    protected void containerIsStarting(final InspectContainerResponse containerInfo) {
        super.containerIsStarting(containerInfo);

        // Get mapped ports for external access
        this.kafkaExposedPort = getMappedPort(KAFKA_PORT);
        this.controllerExposedPort = getMappedPort(CONTROLLER_PORT);

        LOGGER.info("Mapped Kafka port (EXTERNAL listener): {}", kafkaExposedPort);
        LOGGER.info("Mapped controller port: {}", controllerExposedPort);

        // For cluster mode, we need to update the advertised.listeners with the correct mapped port
        // Unfortunately, we can't update environment variables after the container has started,
        // so we need to set them before starting. This is a limitation of using environment variables
        // with native Kafka images in TestContainers.
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
     * This uses the EXTERNAL listener for cluster mode (port 9092) and PLAINTEXT for standalone.
     *
     * @return Kafka bootstrap servers for container network
     */
    public String getNetworkBootstrapServers() {
        // For containers on the same network, they should use the network alias
        // In cluster mode, EXTERNAL listener uses port 9092, in standalone PLAINTEXT uses port 9092
        return NETWORK_ALIAS_PREFIX + nodeId + ":" + KAFKA_PORT;
    }

    @Override
    @DoNotMutate
    public String getBootstrapControllers() {
        // Return controller endpoint using the mapped controller port
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
     * Fluent method to configure Kafka using a configuration map.
     * This method automatically converts Kafka property names to environment variables
     * following the Apache Kafka naming convention (e.g., "offsets.topic.replication.factor"
     * becomes "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR").
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * Map<String, String> kafkaConfig = new HashMap<>();
     * kafkaConfig.put("offsets.topic.replication.factor", "1");
     * kafkaConfig.put("transaction.state.log.replication.factor", "1");
     * kafkaConfig.put("process.roles", "broker,controller");
     * kafkaConfig.put("listeners", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093");
     *
     * NativeKafkaContainer kafka = new NativeKafkaContainer()
     *     .withKafkaConfigurationMap(kafkaConfig)
     *     .waitForRunning();
     * }</pre>
     *
     * @param kafkaConfigurationMap map of Kafka configuration properties
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withKafkaConfigurationMap(Map<String, String> kafkaConfigurationMap) {
        this.kafkaConfigurationMap = kafkaConfigurationMap;
        return self();
    }

    /**
     * Fluent method to set the cluster ID.
     * This is used in multi-node clusters to ensure all nodes share the same cluster ID.
     *
     * @param clusterId the cluster ID
     * @return NativeKafkaContainer instance
     */
    public NativeKafkaContainer withClusterId(String clusterId) {
        this.clusterId = clusterId;
        return self();
    }

    /**
     * Fluent method to set the Kafka version.
     * This creates a new container instance with the specified version.
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * NativeKafkaContainer kafka = new NativeKafkaContainer()
     *     .withKafkaVersion("3.9.0")
     *     .waitForRunning();
     * }</pre>
     *
     * @param version the Kafka version (e.g., "4.1.0", "4.0.0")
     * @return a new NativeKafkaContainer instance with the specified version
     */
    public NativeKafkaContainer withKafkaVersion(String version) {
        if (version == null || version.trim().isEmpty()) {
            throw new IllegalArgumentException("Kafka version cannot be null or empty");
        }
        // Create a new image name with the specified version
        String imageWithVersion = DEFAULT_IMAGE_NAME + ":" + version.trim();
        // We need to update the image via the parent class
        this.setDockerImageName(imageWithVersion);
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