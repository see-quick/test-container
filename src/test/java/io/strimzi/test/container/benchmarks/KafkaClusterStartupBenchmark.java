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

    // Collection to store all benchmark results for final table output
    private static final List<PerformanceMeasurer.BenchmarkResult> ALL_RESULTS = new ArrayList<>();

    /**
     * Benchmark separate roles with increasing controller and broker nodes starting from 2 nodes.
     */
    @Test
    void benchmarkSeparateRolesScaling(TestInfo testInfo) {
        LOGGER.info("Starting separate roles scaling benchmark: {}", testInfo.getDisplayName());

        int[][] separateRolesConfigs = {
            {1, 1}, // 1 controller, 1 broker
            {1, 2}, // 1 controller, 2 brokers
            {1, 3}, // 1 controller, 3 brokers
            {3, 3}, // 3 controllers, 3 brokers
        };

        for (int[] config : separateRolesConfigs) {
            int controllers = config[0];
            int brokers = config[1];

            LOGGER.info("Benchmarking separate roles: {} controllers, {} brokers", controllers, brokers);

            PerformanceMeasurer.BenchmarkResult result = runBenchmark(
                String.format("Separate Roles (%dC+%dB)", controllers, brokers),
                () -> new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                    .withSeparatedRoles()
                    .withNumberOfControllers(controllers)
                    .withNumberOfBrokers(brokers)
                    .withSharedNetwork()
                    .build()
            );

            collectResult(result);

            // Ensure startup time is reasonable (less than 180 seconds for larger clusters)
            assertThat("Separate roles startup should complete within reasonable time",
                       result.getAverageStartupTime(),
                       lessThan(Duration.ofSeconds(180)));
        }
    }

    /**
     * Benchmark combined mode with scaling from 1 to 5 replicas.
     */
    @Test
    void benchmarkCombinedModeScaling(TestInfo testInfo) {
        LOGGER.info("Starting combined mode scaling benchmark: {}", testInfo.getDisplayName());

        int[] combinedReplicas = {
            1,
            2,
            3
        };

        for (int replicas : combinedReplicas) {
            LOGGER.info("Benchmarking combined mode: {} replicas", replicas);

            PerformanceMeasurer.BenchmarkResult result = runBenchmark(
                String.format("Combined Mode (%d replicas)", replicas),
                () -> new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                    .withNumberOfBrokers(replicas)
                    .withSharedNetwork()
                    .build()
            );

            collectResult(result);

            // Ensure startup time is reasonable (less than 120 seconds)
            assertThat("Combined mode startup should complete within reasonable time",
                       result.getAverageStartupTime(),
                       lessThan(Duration.ofSeconds(120)));
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
    
    private void collectResult(PerformanceMeasurer.BenchmarkResult result) {
        synchronized (ALL_RESULTS) {
            ALL_RESULTS.add(result);
        }
        LOGGER.info("Completed benchmark: {} - Average: {} ms", result.getName(), result.getAverageStartupTime().toMillis());
    }

    private void logAllResultsAsTable() {
        StringBuilder table = new StringBuilder();

        table.append("\n").append("=".repeat(140)).append("\n");
        table.append("                          BENCHMARK RESULTS SUMMARY TABLE\n");
        table.append("=".repeat(140)).append("\n");
        table.append(String.format("%-35s | %-12s | %-12s | %-12s | %-12s | %-12s | %-12s\n",
                                   "Configuration", "Runs", "Average (ms)", "Min (ms)", "Max (ms)", "Std Dev (ms)", "CV (%)"));
        table.append("-".repeat(140)).append("\n");

        for (PerformanceMeasurer.BenchmarkResult result : ALL_RESULTS) {
            double coefficientOfVariation = (result.getStandardDeviation().toMillis() / (double) result.getAverageStartupTime().toMillis()) * 100;
            table.append(String.format("%-35s | %-12d | %-12d | %-12d | %-12d | %-12d | %-12.1f\n",
                                       result.getName(),
                                       result.getRunCount(),
                                       result.getAverageStartupTime().toMillis(),
                                       result.getMinStartupTime().toMillis(),
                                       result.getMaxStartupTime().toMillis(),
                                       result.getStandardDeviation().toMillis(),
                                       coefficientOfVariation));
        }

        table.append("=".repeat(140)).append("\n");
        table.append(String.format("Total benchmarks completed: %d\n", ALL_RESULTS.size()));
        table.append("=".repeat(140));

        // Print the entire table as a single log message to avoid logger prefixes on each line
        System.out.println(table);
    }

    @FunctionalInterface
    private interface ClusterSupplier {
        StrimziKafkaCluster get();
    }
    
}