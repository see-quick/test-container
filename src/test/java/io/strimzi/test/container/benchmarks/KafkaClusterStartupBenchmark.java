/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container.benchmarks;

import io.strimzi.test.container.StrimziKafkaCluster;
import io.strimzi.test.container.benchmarks.utils.PerformanceMeasurer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

/**
 * Benchmark tests to compare startup performance between combined and separate Kafka node roles.
 * Tests separate roles scaling and combined mode scaling for comparable performance analysis.
 */
public class KafkaClusterStartupBenchmark {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterStartupBenchmark.class);

    // Configuration for benchmark runs
    private static final int WARMUP_RUNS = 2;
    private static final int BENCHMARK_RUNS = 5;

    // Available Kafka versions to benchmark
    private static final String[] KAFKA_VERSIONS = {"4.0.0", "4.1.0"};

    // Collection to store all benchmark results with their Kafka versions for final table output
    private static final List<BenchmarkResultWithVersion> ALL_RESULTS = new ArrayList<>();

    /**
     * Simple wrapper to associate a benchmark result with its Kafka version
     */
    private static class BenchmarkResultWithVersion {
        private final PerformanceMeasurer.BenchmarkResult result;
        private final String kafkaVersion;

        BenchmarkResultWithVersion(PerformanceMeasurer.BenchmarkResult result, String kafkaVersion) {
            this.result = result;
            this.kafkaVersion = kafkaVersion;
        }

        PerformanceMeasurer.BenchmarkResult getResult() {
            return result;
        }

        String getKafkaVersion() {
            return kafkaVersion;
        }
    }

    // Date formatter for log file naming
    private static final DateTimeFormatter LOG_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    // Static timestamp for the entire benchmark session
    private static final String BENCHMARK_TIMESTAMP = LocalDateTime.now().format(LOG_TIMESTAMP_FORMAT);

    // Counter for tracking benchmark iterations
    private static int iterationCounter = 0;

    /**
     * Benchmark dedicated roles with increasing controller and broker nodes across multiple Kafka versions.
     */
    @Test
    void benchmarkDedicatedRolesScaling(TestInfo testInfo) {
        LOGGER.info("Starting dedicated roles scaling benchmark: {}", testInfo.getDisplayName());

        int[][] dedicatedRolesConfigs = {
            {1, 1}, // 1 controller, 1 broker
            {1, 2}, // 1 controller, 2 brokers
            {1, 3}, // 1 controller, 3 brokers
            {3, 3}, // 3 controllers, 3 brokers
        };

        for (String kafkaVersion : KAFKA_VERSIONS) {
            LOGGER.info("Testing with Kafka version: {}", kafkaVersion);

            for (int[] config : dedicatedRolesConfigs) {
                int controllers = config[0];
                int brokers = config[1];

                LOGGER.info("Benchmarking dedicated roles: {} controllers, {} brokers", controllers, brokers);

                String logPath = generateLogPath("dedicated-roles", controllers, brokers, kafkaVersion);
                PerformanceMeasurer.BenchmarkResult result = runBenchmark(
                    String.format("Dedicated Roles (%dC+%dB)", controllers, brokers),
                    () -> new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withKafkaVersion(kafkaVersion)
                        .withDedicatedRoles()
                        .withNumberOfControllers(controllers)
                        .withNumberOfBrokers(brokers)
                        .withSharedNetwork()
                        .withLogCollection(logPath)
                        .build()
                );

                collectResult(result, kafkaVersion);

                // Ensure startup time is reasonable (less than 180 seconds for larger clusters)
                assertThat("Separate roles startup should complete within reasonable time",
                           result.getAverageStartupTime(),
                           lessThan(Duration.ofSeconds(180)));
            }
        }
    }

    /**
     * Benchmark combined mode with scaling from 1 to 3 replicas across multiple Kafka versions.
     */
    @Test
    void benchmarkCombinedModeScaling(TestInfo testInfo) {
        LOGGER.info("Starting combined mode scaling benchmark: {}", testInfo.getDisplayName());

        int[] combinedReplicas = {
            1,
            2,
            3
        };

        for (String kafkaVersion : KAFKA_VERSIONS) {
            LOGGER.info("Testing with Kafka version: {}", kafkaVersion);

            for (int replicas : combinedReplicas) {
                LOGGER.info("Benchmarking combined mode: {} replicas", replicas);

                String logPath = generateLogPath("combined-mode", replicas, 0, kafkaVersion);
                PerformanceMeasurer.BenchmarkResult result = runBenchmark(
                    String.format("Combined Mode (%d replicas)", replicas),
                    () -> new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                        .withKafkaVersion(kafkaVersion)
                        .withNumberOfBrokers(replicas)
                        .withSharedNetwork()
                        .withLogCollection(logPath)
                        .build()
                );

                collectResult(result, kafkaVersion);

                // Ensure startup time is reasonable (less than 120 seconds)
                assertThat("Combined mode startup should complete within reasonable time",
                           result.getAverageStartupTime(),
                           lessThan(Duration.ofSeconds(120)));
            }
        }
    }

    @AfterAll
    static void printFinalResults() {
        KafkaClusterStartupBenchmark instance = new KafkaClusterStartupBenchmark();
        instance.logAllResultsAsTable();
    }

    private PerformanceMeasurer.BenchmarkResult runBenchmark(String name, ClusterSupplier clusterSupplier) {
        return runBenchmark(name, clusterSupplier, BENCHMARK_RUNS, WARMUP_RUNS);
    }
    
    private PerformanceMeasurer.BenchmarkResult runBenchmark(String name, ClusterSupplier clusterSupplier, int runs, int warmupRuns) {
        PerformanceMeasurer measurer = new PerformanceMeasurer();
        return measurer.measureStartupPerformance(name, clusterSupplier::get, runs, warmupRuns);
    }
    
    private void collectResult(PerformanceMeasurer.BenchmarkResult result, String kafkaVersion) {
        synchronized (ALL_RESULTS) {
            ALL_RESULTS.add(new BenchmarkResultWithVersion(result, kafkaVersion));
        }
        LOGGER.info("Completed benchmark: {} - Average: {} ms", result.getName(), result.getAverageStartupTime().toMillis());
    }

    /**
     * Generates a unique log path for each benchmark run with timestamp and iteration number.
     * Format: target/stc/{timestamp}/{mode}/kafka-{version}/{iteration}-run-controllers-{controllers}-brokers-{brokers}
     * or target/stc/{timestamp}/{mode}/kafka-{version}/{iteration}-run-nodes-{nodes} for combined mode
     *
     * @param mode benchmark mode (e.g., "dedicated-roles", "combined-mode")
     * @param primary primary count (controllers for dedicated roles, nodes for combined mode)
     * @param secondary secondary count (brokers for dedicated roles, 0 for combined mode)
     * @param kafkaVersion Kafka version being benchmarked
     * @return formatted log path
     */
    private static synchronized String generateLogPath(String mode, int primary, int secondary, String kafkaVersion) {
        String path;
        if ("dedicated-roles".equals(mode)) {
            path = String.format("target/stc/%s/%s/kafka-%s-%d-run-controllers-%d-brokers-%d/",
                BENCHMARK_TIMESTAMP, mode, kafkaVersion, iterationCounter, primary, secondary);
        } else {
            path = String.format("target/stc/%s/%s/kafka-%s/%d-run-nodes-%d/",
                BENCHMARK_TIMESTAMP, mode, kafkaVersion, iterationCounter, primary);
        }
        iterationCounter++;
        return path;
    }

    private void logAllResultsAsTable() {
        StringBuilder table = new StringBuilder();

        table.append("\n# Benchmark Results Summary\n\n");
        table.append("| Configuration                 | Kafka Version | Runs | Average (ms) | Min (ms) | Max (ms) | Std Dev (ms) | CV (%) |\n");
        table.append("|-------------------------------|---------------|------|--------------|----------|----------|--------------|--------|\n");

        for (BenchmarkResultWithVersion resultWithVersion : ALL_RESULTS) {
            PerformanceMeasurer.BenchmarkResult result = resultWithVersion.getResult();
            double coefficientOfVariation = (result.getStandardDeviation().toMillis() / (double) result.getAverageStartupTime().toMillis()) * 100;

            table.append(String.format("| %-29s | %-13s | %-4d | %-12d | %-8d | %-8d | %-12d | %-6.1f |\n",
                                       result.getName(),
                                       resultWithVersion.getKafkaVersion(),
                                       result.getRunCount(),
                                       result.getAverageStartupTime().toMillis(),
                                       result.getMinStartupTime().toMillis(),
                                       result.getMaxStartupTime().toMillis(),
                                       result.getStandardDeviation().toMillis(),
                                       coefficientOfVariation));
        }

        table.append("\n**Total benchmarks completed:** ").append(ALL_RESULTS.size()).append("\n");

        System.out.println(table);
    }

    @FunctionalInterface
    private interface ClusterSupplier {
        StrimziKafkaCluster get();
    }
    
}