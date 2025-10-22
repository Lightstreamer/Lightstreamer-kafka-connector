
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS) // Risultati in millisecondi
public class MultiplexerBenchmark {

    // @Param({"1", "4", "8"})
    @Param({"1", "2", "4"})
    int threadCount;

    @Param({"500"}) // Numero totale di task
    private int taskCount;

    private int fibo = 25;

    private TaskExecutor<Integer> multiplexer;
    private ExecutorService executor;

    @Setup(Level.Iteration)
    public void setUp() {
        executor = Executors.newFixedThreadPool(threadCount);
        multiplexer = new Multiplexer<Integer>(executor, true);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        multiplexer.shutdown();
    }

    static long calculateFibonacci(int n) {
        if (n <= 1) return n;
        return calculateFibonacci(n - 1) + calculateFibonacci(n - 2);
    }

    @Benchmark
    public void multiplexerExecutor(Blackhole bh) throws InterruptedException {
        for (int i = 0; i < taskCount; i++) {
            multiplexer.execute(null, () -> bh.consume(calculateFibonacci(fibo)));
        }
        multiplexer.waitBatch();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(MultiplexerBenchmark.class.getSimpleName())
                        .warmupIterations(5)
                        .measurementIterations(10)
                        .forks(1)
                        .build();

        new Runner(opt).run();
    }
}
