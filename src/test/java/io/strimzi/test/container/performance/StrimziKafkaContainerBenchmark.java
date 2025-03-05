/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container.performance;

import io.strimzi.test.container.StrimziKafkaContainer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1) // Warmup only once, for 1 second
@Measurement(iterations = 1) // Measure only once, for 1 second
@Fork(1)
public class StrimziKafkaContainerBenchmark {

    @Benchmark
    public void measureKafkaContainerStartup(Blackhole blackhole) {
        try (StrimziKafkaContainer kafkaContainer = new StrimziKafkaContainer()
                .withKraft()
                .withBrokerId(1)
        ) {
            kafkaContainer.start();

            blackhole.consume(kafkaContainer);
        }
    }
}
