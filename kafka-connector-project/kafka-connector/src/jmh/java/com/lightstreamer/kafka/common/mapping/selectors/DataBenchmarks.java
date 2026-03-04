
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark suite for data element to item name conversion operations.
 *
 * <p>Measures the performance of building item names from data elements using different approaches
 * (single element vs. array of elements). Uses JMH for accurate microbenchmarking with configurable
 * parameters to test scalability across different data set sizes.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
public class DataBenchmarks {

    @Param({"1", "3", "5", "10"})
    private int params;

    private Data[] sortedDataArray;

    private String schemaName;

    private Data first;

    /**
     * Sets up benchmark state with random test data.
     *
     * <p>Generates an array of data elements with randomized keys and sorts them by name to
     * simulate real-world data distribution. Uses a fixed random seed for reproducible results
     * across benchmark runs.
     */
    @Setup(Level.Iteration)
    public void setup() {
        System.out.println("Setting up benchmark with params = " + params);
        schemaName = "TestSchema";

        // Generate test data with random keys to avoid any ordering bias
        Random random = new Random(42); // Fixed seed for reproducibility
        sortedDataArray = new Data[params];
        Set<String> usedKeys = new HashSet<>();
        for (int i = 0; i < params; i++) {
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
            sortedDataArray[i] = Data.from(key, value);
        }

        Arrays.sort(sortedDataArray, (d1, d2) -> d1.name().compareTo(d2.name()));
        first = sortedDataArray[0];
    }

    /**
     * Benchmarks building an item name from a single data element and schema name.
     *
     * <p>Measures the performance of the simplest item name construction scenario with a single
     * data element.
     */
    @Benchmark
    public void buildItemNameSingle(Blackhole bh) {
        String result = Data.buildItemNameSingle(first, schemaName);
        bh.consume(result);
    }

    /**
     * Benchmarks building an item name from multiple data elements and schema name.
     *
     * <p>Measures the performance of constructing item names from an array of data elements,
     * testing scalability with increasing data set sizes.
     */
    @Benchmark
    public void buildItemNameArray(Blackhole bh) {
        String result = Data.buildItemName(sortedDataArray, schemaName);
        bh.consume(result);
    }
}
