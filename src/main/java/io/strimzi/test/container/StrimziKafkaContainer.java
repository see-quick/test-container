/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StrimziKafkaContainer is a single-node instance of Kafka using the image from quay.io/strimzi/kafka with the
 * given version. There are two options for how to use it. The first one is using an embedded zookeeper which will run
 * inside Kafka container. The Another option is to use @StrimziZookeeperContainer as an external Zookeeper.
 * The additional configuration for Kafka broker can be injected via constructor. This container is a good fit for
 * integration testing but for more hardcore testing we suggest using @StrimziKafkaCluster.
 */
public class StrimziKafkaContainer extends GenericContainer<StrimziKafkaContainer> {

    private static final Logger LOGGER = LogManager.getLogger(StrimziKafkaContainer.class);

    public static final int KAFKA_PORT = 9092;
    public static final int ZOOKEEPER_PORT = 2181;

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
    private static final String LATEST_KAFKA_VERSION;
    private static final List<String> SUPPORTED_KAFKA_VERSIONS = new ArrayList<>(5);
    private static final String STRIMZI_VERSION;

    private Map<String, String> kafkaConfigurationMap;
    private String externalZookeeperConnect;
    private int kafkaExposedPort;
    private int brokerId;

    static {
        // Reads the supported_kafka.versions for the supported Kafka versions
        String fileName = "config/sample.txt";
        ClassLoader classLoader = StrimziKafkaContainer.class.getClassLoader();

        try (InputStream inputStream = classLoader.getResourceAsStream(fileName)) {

            String result = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            System.out.println(result);

        } catch (IOException e) {
            e.printStackTrace();
        }


        InputStream kafkaVersionsInputStream = StrimziKafkaContainer.class.getResourceAsStream("/kafka-versions.txt");

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(kafkaVersionsInputStream, StandardCharsets.UTF_8))) {
            String kafkaVersion;
            while ((kafkaVersion = bufferedReader.readLine()) != null) {
                SUPPORTED_KAFKA_VERSIONS.add(kafkaVersion);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOGGER.info("Supported Kafka versions: {}", SUPPORTED_KAFKA_VERSIONS);

        // sort kafka version from low to high
        Collections.sort(SUPPORTED_KAFKA_VERSIONS);

        LATEST_KAFKA_VERSION = SUPPORTED_KAFKA_VERSIONS.get(SUPPORTED_KAFKA_VERSIONS.size() - 1);

        // Reads the strimzi-version.txt for the Strimzi version which should be used
        InputStream strimziVersionsInputStream = StrimziKafkaContainer.class.getResourceAsStream("/strimzi-version.txt");
        String strimziVersion = null;

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(strimziVersionsInputStream, StandardCharsets.UTF_8))) {
            strimziVersion = bufferedReader.readLine();

            if (strimziVersion == null)    {
                throw new RuntimeException("Failed to read Strimzi version");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        STRIMZI_VERSION = strimziVersion;
        LOGGER.info("Supported Strimzi version: {}", STRIMZI_VERSION);
    }

    private StrimziKafkaContainer(final int brokerId, final String imageVersion, Map<String, String> additionalKafkaConfiguration) {
        super("quay.io/strimzi/kafka:" + imageVersion);
        super.withNetwork(Network.SHARED);

        this.brokerId = brokerId;

        kafkaConfigurationMap = new HashMap<>(additionalKafkaConfiguration);
        kafkaConfigurationMap.put("broker.id", String.valueOf(this.brokerId));

        // exposing kafka port from the container
        withExposedPorts(KAFKA_PORT);

        withEnv("LOG_DIR", "/tmp");
    }

    public static StrimziKafkaContainer createWithExternalZookeeper(final int brokerId, final String imageVersion,
                                                                    final String connectString, Map<String, String> additionalKafkaConfiguration) {
        return new StrimziKafkaContainer(brokerId, imageVersion, additionalKafkaConfiguration)
            .withExternalZookeeper(connectString);
    }

    public static StrimziKafkaContainer createWithAdditionalConfiguration(final int brokerId, final String imageVersion, Map<String, String> additionalKafkaConfiguration) {
        return new StrimziKafkaContainer(brokerId, imageVersion, additionalKafkaConfiguration);
    }

    public static StrimziKafkaContainer createWithAdditionalConfiguration(final int brokerId, Map<String, String> additionalKafkaConfiguration) {
        return new StrimziKafkaContainer(brokerId, STRIMZI_VERSION + "-kafka-" + LATEST_KAFKA_VERSION, additionalKafkaConfiguration);
    }

    public static StrimziKafkaContainer create(final int brokerId) {
        return new StrimziKafkaContainer(brokerId, STRIMZI_VERSION + "-kafka-" + LATEST_KAFKA_VERSION, Collections.emptyMap());
    }

    @Override
    protected void doStart() {
        // we need it for the startZookeeper(); and startKafka(); to run container before...
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    public StrimziKafkaContainer withExternalZookeeper(String connectString) {
        this.externalZookeeperConnect = connectString;
        return self();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        this.kafkaExposedPort = getMappedPort(KAFKA_PORT);

        LOGGER.info("This is mapped port {}", this.kafkaExposedPort);

        StringBuilder advertisedListeners = new StringBuilder(getBootstrapServers());

        Collection<ContainerNetwork> cns = containerInfo.getNetworkSettings().getNetworks().values();

        int advertisedListenerNumber = 1;
        List<String> advertisedListenersNames = new ArrayList<>();

        for (ContainerNetwork cn : cns) {
            // must be always unique
            final String advertisedName = "BROKER" + advertisedListenerNumber;
            advertisedListeners.append(",").append(advertisedName).append("://").append(cn.getIpAddress()).append(":9093");
            advertisedListenersNames.add(advertisedName);
            advertisedListenerNumber++;
        }

        LOGGER.info("This is all advertised listeners for Kafka {}", advertisedListeners.toString());

        StringBuilder kafkaListeners = new StringBuilder();
        StringBuilder kafkaListenerSecurityProtocol = new StringBuilder();

        advertisedListenersNames.forEach(name -> {
            // listeners
            kafkaListeners.append(name);
            kafkaListeners.append("://0.0.0.0:9093");
            kafkaListeners.append(",");
            // listener.security.protocol.map
            kafkaListenerSecurityProtocol.append(name);
            kafkaListenerSecurityProtocol.append(":PLAINTEXT");
            kafkaListenerSecurityProtocol.append(",");
        });

        StringBuilder kafkaConfiguration = new StringBuilder();

        // default listener config
        kafkaConfiguration
            .append(" --override listeners=" + kafkaListeners + "PLAINTEXT://0.0.0.0:" + KAFKA_PORT)
            .append(" --override advertised.listeners=" + advertisedListeners)
            .append(" --override zookeeper.connect=localhost:" + ZOOKEEPER_PORT)
            .append(" --override listener.security.protocol.map=" + kafkaListenerSecurityProtocol + "PLAINTEXT:PLAINTEXT")
            .append(" --override inter.broker.listener.name=BROKER1");

        // additional kafka config
        this.kafkaConfigurationMap.forEach((configName, configValue) -> {
            kafkaConfiguration.append(" --override " + configName + "=" + configValue);
        });

        String command = "#!/bin/bash \n";

        if (this.externalZookeeperConnect != null) {
            withEnv("KAFKA_ZOOKEEPER_CONNECT", this.externalZookeeperConnect);
        } else {
            command += "bin/zookeeper-server-start.sh config/zookeeper.properties &\n";
        }
        command += "bin/kafka-server-start.sh config/server.properties" + kafkaConfiguration.toString();

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
            Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
            STARTER_SCRIPT
        );
    }

    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), this.kafkaExposedPort);
    }

    public static List<String> getSupportedKafkaVersions() {
        return SUPPORTED_KAFKA_VERSIONS;
    }

    public static String getLatestKafkaVersion() {
        return LATEST_KAFKA_VERSION;
    }

    public static String getStrimziVersion() {
        return STRIMZI_VERSION;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
