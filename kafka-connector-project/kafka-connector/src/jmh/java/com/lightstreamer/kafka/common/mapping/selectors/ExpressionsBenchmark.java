
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.common.mapping.selectors;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
public class ExpressionsBenchmark {

    @Param({"1", "3", "5", "10"})
    private int params;

    private String schemaName;
    private String testString;

    @Setup(Level.Iteration)
    public void setup() {
        System.out.println("Setting up benchmark with params = " + params);
        schemaName = "TestSchema";

        // Generate test data with random keys to avoid any ordering bias
        Random random = new Random(42); // Fixed seed for reproducibility
        Set<String> usedKeys = new HashSet<>();

        StringBuilder sb = new StringBuilder(schemaName);
        sb.append("-[");
        for (int i = 0; i < params; i++) {
            if (i > 0) {
                sb.append(",");
            }
            String key;
            do {
                key =
                        "key_"
                                + String.format(
                                        "%03d",
                                        random.nextInt(10000)); // Larger range to reduce collisions
            } while (usedKeys.contains(key));

            usedKeys.add(key);
            String value = "value_" + i;
            sb.append(String.format("%s=%s", key, value));
        }
        sb.append("]");
        testString = sb.toString();
    }

    /**
     * Benchmarks the performance of converting a subscription expression to its canonical item name
     * representation.
     *
     * <p>This benchmark measures the execution time of the {@link Expressions#Subscription(String)}
     * method combined with the {@link #asCanonicalItemName()} operation on the resulting expression
     * object. The result is consumed by the blackhole to prevent dead code elimination and ensure
     * accurate performance measurements.
     *
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public void subscriptionAsCanonicalItemName(Blackhole bh) {
        String result = Expressions.Subscription(testString).asCanonicalItemName();
        bh.consume(result);
    }

    /**
     * Benchmarks the performance of the {@link Expressions#CanonicalItemName(String)} method.
     *
     * <p>This benchmark measures the execution time of converting a test string to its canonical
     * item name representation. The result is consumed by the blackhole to prevent dead code
     * elimination and ensure accurate performance measurements.
     *
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public void canonicalItemName(Blackhole bh) {
        String result = Expressions.CanonicalItemName(testString);
        bh.consume(result);
    }
}
