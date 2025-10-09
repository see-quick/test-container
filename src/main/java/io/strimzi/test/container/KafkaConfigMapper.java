/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting Kafka configuration properties to environment variable format.
 * This mapper follows the Apache Kafka native image convention for environment variable mapping:
 * <ul>
 *   <li>Replace {@code .} with {@code _}</li>
 *   <li>Replace {@code _} with {@code __} (double underscore)</li>
 *   <li>Replace {@code -} with {@code ___} (triple underscore)</li>
 *   <li>Prefix the result with {@code KAFKA_}</li>
 *   <li>Convert to uppercase</li>
 * </ul>
 *
 * <p>Examples:</p>
 * <ul>
 *   <li>{@code abc.def} becomes {@code KAFKA_ABC_DEF}</li>
 *   <li>{@code abc-def} becomes {@code KAFKA_ABC___DEF}</li>
 *   <li>{@code abc_def} becomes {@code KAFKA_ABC__DEF}</li>
 * </ul>
 *
 * @see <a href="https://github.com/apache/kafka/blob/trunk/docker/examples/README.md">Apache Kafka Docker Documentation</a>
 */
class KafkaConfigMapper {

    private KafkaConfigMapper() {
        // Utility class
    }

    /**
     * Converts a map of Kafka configuration properties to environment variables.
     * Each property name is transformed according to Kafka's environment variable naming convention.
     *
     * @param kafkaConfig Map of Kafka configuration properties (e.g., {@code offsets.topic.replication.factor=1})
     * @return Map of environment variables (e.g., {@code KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1})
     */
    public static Map<String, String> toEnvironmentVariables(Map<String, String> kafkaConfig) {
        if (kafkaConfig == null || kafkaConfig.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, String> envVars = new HashMap<>();
        kafkaConfig.forEach((key, value) -> {
            String envVarName = toEnvironmentVariableName(key);
            envVars.put(envVarName, value);
        });
        return envVars;
    }

    /**
     * Converts a single Kafka property name to its environment variable equivalent.
     *
     * Transformation rules:
     * <ol>
     *   <li>Replace all underscores ({@code _}) with double underscores ({@code __})</li>
     *   <li>Replace all hyphens ({@code -}) with triple underscores ({@code ___})</li>
     *   <li>Replace all dots ({@code .}) with single underscores ({@code _})</li>
     *   <li>Convert to uppercase</li>
     *   <li>Prefix with {@code KAFKA_}</li>
     * </ol>
     *
     * @param propertyName Kafka property name (e.g., {@code offsets.topic.replication.factor})
     * @return Environment variable name (e.g., {@code KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR})
     */
    public static String toEnvironmentVariableName(String propertyName) {
        if (propertyName == null || propertyName.isEmpty()) {
            throw new IllegalArgumentException("Property name cannot be null or empty");
        }

        // Order matters: replace underscores first, then hyphens, then dots
        String result = propertyName
            .replace("_", "__")      // underscore -> double underscore
            .replace("-", "___")     // hyphen -> triple underscore
            .replace(".", "_")       // dot -> underscore
            .toUpperCase(java.util.Locale.ROOT);          // convert to uppercase

        return "KAFKA_" + result;
    }

    /**
     * Converts an environment variable name back to a Kafka property name.
     * This is the reverse operation of {@link #toEnvironmentVariableName(String)}.
     *
     * @param envVarName Environment variable name (e.g., {@code KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR})
     * @return Kafka property name (e.g., {@code offsets.topic.replication.factor})
     */
    public static String toPropertyName(String envVarName) {
        if (envVarName == null || envVarName.isEmpty()) {
            throw new IllegalArgumentException("Environment variable name cannot be null or empty");
        }

        if (!envVarName.startsWith("KAFKA_")) {
            throw new IllegalArgumentException("Environment variable must start with KAFKA_ prefix");
        }

        // Remove KAFKA_ prefix and convert to lowercase
        String result = envVarName.substring(6).toLowerCase(java.util.Locale.ROOT);

        // Order matters for reverse operation: replace triple underscores first, then double, then single
        result = result.replace("___", "-")     // triple underscore -> hyphen
                       .replace("__", "_")      // double underscore -> underscore
                       .replace("_", ".");      // underscore -> dot

        return result;
    }
}