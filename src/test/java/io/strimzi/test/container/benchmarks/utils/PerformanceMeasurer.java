/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container.benchmarks.utils;

import io.strimzi.test.container.StrimziKafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Utility class for measuring performance of Kafka cluster operations.
 * Provides methods for timing cluster startup and collecting performance metrics.
 */
public class PerformanceMeasurer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceMeasurer.class);
    
    /**
     * Measures the startup performance of a Kafka cluster configuration.
     * 
     * @param name descriptive name for the benchmark
     * @param clusterSupplier supplier that creates the cluster to test
     * @param runs number of benchmark runs to perform
     * @param warmupRuns number of warmup runs before actual measurements
     * @return benchmark result containing timing statistics
     */
    public BenchmarkResult measureStartupPerformance(String name, 
                                                    Supplier<StrimziKafkaCluster> clusterSupplier,
                                                    int runs, 
                                                    int warmupRuns) {
        LOGGER.info("Starting performance measurement for: {}", name);
        LOGGER.info("Configuration: {} warmup runs, {} benchmark runs", warmupRuns, runs);
        
        List<Duration> startupTimes = new ArrayList<>();
        
        // Perform warmup runs
        performWarmupRuns(name, clusterSupplier, warmupRuns);
        
        // Perform actual benchmark runs
        for (int i = 0; i < runs; i++) {
            LOGGER.info("Benchmark run {} of {} for {}", i + 1, runs, name);
            
            Duration startupTime = measureSingleStartup(clusterSupplier);
            startupTimes.add(startupTime);
            
            LOGGER.info("Run {} completed in: {} ms", i + 1, startupTime.toMillis());
            
            // Small delay between runs to ensure clean state
            waitBetweenRuns();
        }
        
        BenchmarkResult result = new BenchmarkResult(name, startupTimes);
        logBenchmarkSummary(result);
        
        return result;
    }
    
    /**
     * Measures the time it takes to start a single cluster instance.
     * 
     * @param clusterSupplier supplier that creates the cluster to test
     * @return duration of the startup process
     */
    public Duration measureSingleStartup(Supplier<StrimziKafkaCluster> clusterSupplier) {
        StrimziKafkaCluster cluster = null;

        try {
            LOGGER.debug("Creating cluster instance");
            cluster = clusterSupplier.get();

            LOGGER.debug("Starting cluster measurement");
            Instant start = Instant.now();

            cluster.start();

            Instant end = Instant.now();
            Duration startupTime = Duration.between(start, end);

            LOGGER.debug("Cluster started successfully in {} ms", startupTime.toMillis());

            // Allow some time for cluster stabilization
            waitForStabilization();

            return startupTime;

        } catch (Exception e) {
            LOGGER.error("Failed to start cluster during measurement", e);
            throw new RuntimeException("Cluster startup failed", e);
        } finally {
            if (cluster != null) {
                try {
                    LOGGER.debug("Stopping cluster and cleaning up resources");
                    cluster.stop();

                    // Additional cleanup time to ensure containers are fully removed
                    LOGGER.debug("Waiting for container cleanup to complete");
                    TimeUnit.SECONDS.sleep(3);

                } catch (Exception e) {
                    LOGGER.warn("Error during cluster cleanup: {}", e.getMessage());
                }
            }
        }
    }
    
    private void performWarmupRuns(String name, Supplier<StrimziKafkaCluster> clusterSupplier, int warmupRuns) {
        if (warmupRuns <= 0) {
            LOGGER.info("Skipping warmup runs for {}", name);
            return;
        }
        
        LOGGER.info("Performing {} warmup runs for {}", warmupRuns, name);
        
        for (int i = 0; i < warmupRuns; i++) {
            LOGGER.info("Warmup run {} of {} for {}", i + 1, warmupRuns, name);
            
            try {
                Duration warmupTime = measureSingleStartup(clusterSupplier);
                LOGGER.info("Warmup run {} completed in: {} ms", i + 1, warmupTime.toMillis());
            } catch (Exception e) {
                LOGGER.warn("Warmup run {} failed: {}", i + 1, e.getMessage());
            }
            
            waitBetweenRuns();
        }
        
        LOGGER.info("Warmup completed for {}", name);
    }
    
    private void waitForStabilization() {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted during stabilization wait");
        }
    }
    
    private void waitBetweenRuns() {
        try {
            LOGGER.info("Waiting 15 seconds between runs for thorough system cleanup...");
            // Force garbage collection to clean up any remaining resources
            System.gc();
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted during wait between runs");
        }
    }
    
    private void logBenchmarkSummary(BenchmarkResult result) {
        LOGGER.info("=== Performance Measurement Summary: {} ===", result.getName());
        LOGGER.info("Total runs: {}", result.getRunCount());
        LOGGER.info("Average startup time: {} ms", result.getAverageStartupTime().toMillis());
        LOGGER.info("Minimum startup time: {} ms", result.getMinStartupTime().toMillis());
        LOGGER.info("Maximum startup time: {} ms", result.getMaxStartupTime().toMillis());
        LOGGER.info("Standard deviation: {} ms", result.getStandardDeviation().toMillis());
        
        // Calculate coefficient of variation for consistency analysis
        double coefficientOfVariation = (double) result.getStandardDeviation().toMillis() / 
                                      result.getAverageStartupTime().toMillis() * 100;
        LOGGER.info(String.format("Coefficient of variation: %.1f%%", coefficientOfVariation));
        
        if (coefficientOfVariation > 20) {
            LOGGER.warn("High variability detected (CV > 20%) - consider increasing warmup runs");
        }
        
        LOGGER.info("===============================================");
    }
    
    /**
     * Container class for benchmark results with statistical analysis capabilities.
     */
    public static class BenchmarkResult {
        private final String name;
        private final List<Duration> startupTimes;
        
        public BenchmarkResult(String name, List<Duration> startupTimes) {
            this.name = name;
            this.startupTimes = new ArrayList<>(startupTimes);
        }
        
        public String getName() {
            return name;
        }
        
        public List<Duration> getStartupTimes() {
            return new ArrayList<>(startupTimes);
        }
        
        public int getRunCount() {
            return startupTimes.size();
        }
        
        public Duration getAverageStartupTime() {
            if (startupTimes.isEmpty()) {
                return Duration.ZERO;
            }
            
            return Duration.ofMillis(
                startupTimes.stream()
                    .mapToLong(Duration::toMillis)
                    .sum() / startupTimes.size()
            );
        }
        
        public Duration getMinStartupTime() {
            return startupTimes.stream()
                .min(Duration::compareTo)
                .orElse(Duration.ZERO);
        }
        
        public Duration getMaxStartupTime() {
            return startupTimes.stream()
                .max(Duration::compareTo)
                .orElse(Duration.ZERO);
        }
        
        public Duration getStandardDeviation() {
            if (startupTimes.size() <= 1) {
                return Duration.ZERO;
            }
            
            double mean = getAverageStartupTime().toMillis();
            double variance = startupTimes.stream()
                .mapToDouble(Duration::toMillis)
                .map(time -> Math.pow(time - mean, 2))
                .average()
                .orElse(0.0);
                
            return Duration.ofMillis((long) Math.sqrt(variance));
        }
    }
}