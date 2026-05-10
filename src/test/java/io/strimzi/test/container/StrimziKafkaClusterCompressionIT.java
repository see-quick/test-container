/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Demonstrates that the LogCleaner ignores topic-configured compression level
 * during log compaction, always using default compression parameters instead.
 *
 * Related: KAFKA-7632 (KIP-390), KAFKA-20036, KIP-780
 */
public class StrimziKafkaClusterCompressionIT extends AbstractIT {

    private StrimziKafkaCluster systemUnderTest;

    @AfterEach
    void afterEach() {
        if (this.systemUnderTest != null) {
            this.systemUnderTest.stop();
        }
    }

    /**
     * Demonstrates that the LogCleaner re-compresses batches using the
     * topic-configured compression level when it removes some records
     * from a batch during compaction.
     *
     * The key insight: the cleaner only calls buildRetainedRecordsInto()
     * (which applies the topic compression level) when it PARTIALLY cleans
     * a batch i.e., removing some records but keeping others. If all records in
     * a batch survive, the batch is copied verbatim with no re-compression.
     *
     * To trigger partial batch cleaning, we produce multi-key batches
     * (many keys per batch via large linger.ms/batch.size) then overwrite
     * only a subset of keys. The old batches contain a mix of "stale" keys
     * (overwritten) and "fresh" keys (not overwritten), forcing the cleaner
     * to rebuild each batch with the topic-configured compression level.
     */
    @Test
    void testLogCleanerIgnoresTopicCompressionLevel() throws Exception {
        Map<String, String> kafkaConfig = new HashMap<>();
        kafkaConfig.put("log.cleaner.enable", "true");
        kafkaConfig.put("log.cleaner.backoff.ms", "500");
        kafkaConfig.put("log.cleaner.min.cleanable.ratio", "0.01");
        kafkaConfig.put("log.initial.task.delay.ms", "0");

        systemUnderTest = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
            .withNumberOfBrokers(1)
            .withAdditionalKafkaConfiguration(kafkaConfig)
            // if you comment this out then you can see problem (i.e., 0% difference between compressions as it would use default level for both topics)
            .withImage("quay.io/morsak/test-container:latest-kafka-4.4.0-SNAPSHOT-aarch64")
            .withLogCollection()
            .withContainerCustomizer(containerCustomizer -> containerCustomizer.withKafkaLog(Level.DEBUG))
            .build();

        systemUnderTest.start();

        final String topicLow = "compact-gzip-level-1";
        final String topicHigh = "compact-gzip-level-9";

        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers()))) {

            Map<String, String> commonConfig = Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip",
                TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0",
                TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "1000",
                TopicConfig.SEGMENT_BYTES_CONFIG, "1048576",
                TopicConfig.SEGMENT_MS_CONFIG, "1000"
            );

            Map<String, String> configLow = new HashMap<>(commonConfig);
            configLow.put("compression.gzip.level", "1");

            Map<String, String> configHigh = new HashMap<>(commonConfig);
            configHigh.put("compression.gzip.level", "9");

            admin.createTopics(List.of(
                new NewTopic(topicLow, 1, (short) 1).configs(configLow),
                new NewTopic(topicHigh, 1, (short) 1).configs(configHigh)
            )).all().get(30, TimeUnit.SECONDS);
        }

        produceInitialData(topicLow, 42);
        produceInitialData(topicHigh, 42);

        // Wait for initial segments to roll so they become eligible for compaction
        System.out.println("=== Waiting for initial segments to roll... ===");
        Thread.sleep(3000);

        // Overwrite only half the keys i.e., this makes the old batches "dirty":
        // each old batch has ~50% stale records (overwritten keys) and ~50%
        // fresh records (non-overwritten keys), forcing the cleaner to rebuild
        // each batch via buildRetainedRecordsInto() with topic compression level.
        produceOverwriteData(topicLow, 99);
        produceOverwriteData(topicHigh, 99);

        System.out.println("=== Waiting for overwrite segments to roll... ===");
        Thread.sleep(3000);

        System.out.println("=== Segments BEFORE compaction ===");
        dumpSegments(topicLow);
        dumpSegments(topicHigh);

        System.out.println("=== Waiting for compaction to complete... ===");
        waitForCompactionComplete(topicLow);
        waitForCompactionComplete(topicHigh);

        System.out.println("=== Waiting for compaction to stabilize... ===");
        waitForCompactionToStabilize(topicLow);
        waitForCompactionToStabilize(topicHigh);

        System.out.println("=== Segments AFTER compaction ===");
        long sizeLow = getCompactedSegmentSize(topicLow);
        long sizeHigh = getCompactedSegmentSize(topicHigh);
        dumpSegments(topicLow);
        dumpSegments(topicHigh);

        System.out.println("=== Compacted segment sizes ===");
        System.out.println("Level 1  topic: " + sizeLow + " bytes");
        System.out.println("Level 9  topic: " + sizeHigh + " bytes");

        System.out.println("=== Broker cleaner logs ===");
        dumpCleanerLogs();

        System.out.println("=== kafka-dump-log level=1 topic ===");
        dumpLogDetails(topicLow);
        System.out.println("=== kafka-dump-log level=9 topic ===");
        dumpLogDetails(topicHigh);

        double diffPercent = Math.abs(sizeLow - sizeHigh) * 100.0 / Math.max(sizeLow, 1);
        System.out.println("=== Size difference: " + String.format("%.2f", diffPercent) + "% ===");
        System.out.println("If the cleaner honors topic levels, we'd expect >5% difference (fix works).");
        System.out.println("If sizes are nearly identical (<5%), the cleaner ignores the configured level (bug).");

        // With the fix, level=9 compacted segments should be smaller
        // than level=1, producing a measurable size difference (>10%).
        // Without the fix, sizes would be nearly identical (<1%).
        assertThat(
            "Compacted segment sizes differ by " +
                String.format("%.2f", diffPercent) + "%. Level1=" + sizeLow +
                " Level9=" + sizeHigh + ". The LogCleaner correctly applies " +
                "topic-configured compression level during compaction.",
            diffPercent,
            greaterThan(10.0));
    }

    /**
     * Produces initial data with all keys.
     * Uses large linger.ms and batch.size so many keys land in each batch.
     */
    private void produceInitialData(String topicName, long seed) throws Exception {
        int totalKeys = 1000;
        int rounds = 10;
        Map<String, Object> producerConfigs = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip",
            ProducerConfig.LINGER_MS_CONFIG, "500",
            ProducerConfig.BATCH_SIZE_CONFIG, "1048576"
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                producerConfigs, new StringSerializer(), new StringSerializer())) {

            Random rng = new Random(seed);
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int round = 0; round < rounds; round++) {
                for (int key = 0; key < totalKeys; key++) {
                    String value = generateCompressibleValue(rng, round, key);
                    futures.add(producer.send(new ProducerRecord<>(topicName,
                        "key-" + key, value)));
                }
                producer.flush();
                for (var f : futures) {
                    f.get();
                }
                futures.clear();
            }
        }
    }

    /**
     * Overwrites a subset of the key space, leaving the rest with their original
     * values in old batches. This creates "dirty" batches where the cleaner must
     * remove stale records and re-compress the survivors.
     */
    private void produceOverwriteData(String topicName, long seed) throws Exception {
        int overwriteKeys = 500;
        int rounds = 5;
        Map<String, Object> producerConfigs = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemUnderTest.getBootstrapServers(),
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip",
            ProducerConfig.LINGER_MS_CONFIG, "500",
            ProducerConfig.BATCH_SIZE_CONFIG, "1048576"
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(
                producerConfigs, new StringSerializer(), new StringSerializer())) {

            Random rng = new Random(seed);
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int round = 0; round < rounds; round++) {
                for (int key = 0; key < overwriteKeys; key++) {
                    String value = generateCompressibleValue(rng, round + 100, key);
                    futures.add(producer.send(new ProducerRecord<>(topicName,
                        "key-" + key, value)));
                }
                producer.flush();
                for (var f : futures) {
                    f.get();
                }
                futures.clear();
            }
        }
    }

    private String generateCompressibleValue(Random rng, int round, int key) {
        int targetSize = 8192;
        StringBuilder sb = new StringBuilder(targetSize);
        int entryId = 0;
        String[] eventTypes = {
            "kafka.broker.metrics.snapshot",
            "kafka.broker.request.completed",
            "kafka.broker.replication.status",
            "kafka.broker.partition.reassignment",
            "kafka.broker.controller.election"
        };
        String[] hosts = {
            "broker-0.kafka.svc.cluster.local",
            "broker-1.kafka.svc.cluster.local",
            "broker-2.kafka.svc.cluster.local"
        };
        String[] racks = {"us-east-1a", "us-east-1b", "us-west-2a", "eu-west-1a"};
        String[] descriptions = {
            "Periodic broker metrics snapshot for capacity planning and performance monitoring dashboard",
            "Request processing completed with detailed latency breakdown and throughput measurements",
            "Replication status report including ISR changes and under-replicated partition details",
            "Partition reassignment progress update with bandwidth throttling and completion estimates",
            "Controller election event with candidate broker information and epoch transition details"
        };
        while (sb.length() < targetSize) {
            int type = entryId % eventTypes.length;
            sb.append("{\"eventType\":\"").append(eventTypes[type]).append("\",")
                .append("\"round\":").append(round).append(",")
                .append("\"key\":").append(key).append(",")
                .append("\"entryId\":").append(entryId++).append(",")
                .append("\"timestamp\":\"2026-05-08T12:").append(String.format("%02d", entryId % 60))
                .append(":").append(String.format("%02d", rng.nextInt(60))).append(".000Z\",")
                .append("\"source\":{")
                .append("\"host\":\"").append(hosts[rng.nextInt(hosts.length)]).append("\",")
                .append("\"port\":").append(9092 + rng.nextInt(3)).append(",")
                .append("\"rack\":\"").append(racks[rng.nextInt(racks.length)]).append("\",")
                .append("\"dataCenter\":\"").append(racks[rng.nextInt(racks.length)]).append("-dc").append(rng.nextInt(3)).append("\"")
                .append("},")
                .append("\"metrics\":{")
                .append("\"bytesInPerSec\":").append(rng.nextInt(100000)).append(",")
                .append("\"bytesOutPerSec\":").append(rng.nextInt(100000)).append(",")
                .append("\"messagesInPerSec\":").append(rng.nextInt(10000)).append(",")
                .append("\"totalFetchRequestsPerSec\":").append(rng.nextInt(5000)).append(",")
                .append("\"totalProduceRequestsPerSec\":").append(rng.nextInt(5000)).append(",")
                .append("\"requestLatencyMsP50\":").append(rng.nextInt(100)).append(",")
                .append("\"requestLatencyMsP99\":").append(rng.nextInt(500)).append(",")
                .append("\"requestLatencyMsP999\":").append(rng.nextInt(2000))
                .append("},")
                .append("\"partitionInfo\":{")
                .append("\"leaderPartitions\":").append(rng.nextInt(200)).append(",")
                .append("\"followerPartitions\":").append(rng.nextInt(200)).append(",")
                .append("\"underReplicatedPartitions\":").append(rng.nextInt(5)).append(",")
                .append("\"offlinePartitions\":").append(rng.nextInt(2))
                .append("},")
                .append("\"tags\":[\"production\",\"tier-1\",\"kafka-cluster-main\",\"monitoring-enabled\",\"auto-scaling-eligible\"],")
                .append("\"description\":\"").append(descriptions[type]).append("\"")
                .append("}\n");
        }
        return sb.toString();
    }

    private long getCompactedSegmentSize(String topicName) throws Exception {
        String logDir = "/tmp/default-log-dir/" + topicName + "-0";
        Container.ExecResult result = execWithRetry(
            "du -sb " + logDir + "/*.log 2>/dev/null" +   // get byte size of each .log file
            " | sort -k2" +                               // sort by filename (= by offset)
            " | head -n -1" +                             // drop last line (active segment) => because that is not compacted :))
            " | awk '{sum+=$1} END{print sum}'"           // sum the sizes
        );
        String output = result.getStdout().trim();
        if (output.isEmpty()) {
            return 0;
        }
        return Long.parseLong(output);
    }

    private void dumpSegments(String topicName) throws Exception {
        Container.ExecResult result = execWithRetry(
            "echo '--- " + topicName + " ---'; " +
                "ls -la /tmp/default-log-dir/" + topicName + "-0/*.log 2>/dev/null; " +
                "du -sb /tmp/default-log-dir/" + topicName + "-0/*.log 2>/dev/null"
        );
        System.out.println(result.getStdout());
    }

    private void dumpLogDetails(String topicName) throws Exception {
        Container.ExecResult result = execWithRetry(
            "for f in /tmp/default-log-dir/" + topicName + "-0/*.log; do " +
                "echo \"--- $f ---\"; " +
                "bin/kafka-dump-log.sh --files $f --print-data-log 2>/dev/null | head -5; " +
                "done"
        );
        System.out.println(result.getStdout());
    }

    private void waitForCompactionToStabilize(String topicName) throws Exception {
        long previousSize = -1;
        int stableCount = 0;
        long deadline = System.currentTimeMillis() + 120000;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(5000);
            long currentSize = getCompactedSegmentSize(topicName);
            System.out.println("  " + topicName + ": " + currentSize + " bytes");
            if (currentSize == previousSize) {
                stableCount++;
                if (stableCount >= 3) {
                    System.out.println("  " + topicName + ": compaction stabilized at " + currentSize + " bytes");
                    return;
                }
            } else {
                stableCount = 0;
            }
            previousSize = currentSize;
        }
        System.out.println("  " + topicName + ": timed out waiting for stabilization, proceeding anyway");
    }

    private void waitForCompactionComplete(String topicName) throws Exception {
        String partition = topicName + "-0";
        long deadline = System.currentTimeMillis() + 120000;
        while (System.currentTimeMillis() < deadline) {
            String logs = systemUnderTest.getBrokers().stream().findFirst().get().getLogs();
            if (containsCompactionEvidence(logs, partition)) {
                System.out.println("  " + topicName + ": compaction detected in broker logs");
                return;
            }
            Thread.sleep(3000);
            System.out.println("  " + topicName + ": waiting for compaction to start...");
        }
        System.out.println("  " + topicName + ": WARNING — no compaction log entries found after 120s");
    }

    private boolean containsCompactionEvidence(String logs, String partition) {
        boolean cleaningStarted = logs.contains("Beginning cleaning of log " + partition)
            || logs.contains("Cleaning log " + partition);
        boolean cleaningDone = logs.contains("cleaned log " + partition);
        return cleaningStarted || cleaningDone;
    }

    private void dumpCleanerLogs() {
        String logs = systemUnderTest.getBrokers().stream().findFirst().get().getLogs();
        for (String line : logs.split("\n")) {
            String lower = line.toLowerCase(java.util.Locale.ENGLISH);
            if (isCleanerLogLine(lower)) {
                System.out.println("  CLEANER: " + line);
            }
        }
    }

    private boolean isCleanerLogLine(String lowerCaseLine) {
        return lowerCaseLine.contains("cleaner") || lowerCaseLine.contains("cleaning")
            || lowerCaseLine.contains("compaction");
    }

    private Container.ExecResult execWithRetry(String command) throws Exception {
        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return systemUnderTest.getBrokers().stream().findFirst().get().execInContainer("bash", "-c", command);
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    throw e;
                }
                System.out.println("execInContainer failed (attempt " + attempt + "/" + maxRetries + "), retrying...");
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("unreachable");
    }
}