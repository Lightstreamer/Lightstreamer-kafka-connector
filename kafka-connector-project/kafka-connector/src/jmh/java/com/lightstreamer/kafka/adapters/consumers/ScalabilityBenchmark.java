
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

package com.lightstreamer.kafka.adapters.consumers;

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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput) // Misura il numero di operazioni completate al secondo
@OutputTimeUnit(TimeUnit.MILLISECONDS) // Risultati in millisecondi
@State(Scope.Thread) // Stato isolato per ogni thread
public class ScalabilityBenchmark {

    @Param({"1", "2", "4"})
    private int threadCount;

    @Param({"500"})
    private int taskCount;

    private ExecutorService executorService;

    private int fibo = 25;

    @Setup(Level.Iteration)
    public void setup() {
        executorService = Executors.newFixedThreadPool(threadCount);
    }

    @TearDown(Level.Iteration)
    public void teardown() throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
            System.out.println("Forcing shutdown");
            executorService.shutdownNow();
        }
    }

    static long calculateFibonacci(int n) {
        if (n <= 1) return n;
        return calculateFibonacci(n - 1) + calculateFibonacci(n - 2);
    }

    @Benchmark
    public void testScalability(Blackhole bh) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(taskCount);
        for (int i = 0; i < this.taskCount; i++) {
            executorService.submit(
                    () -> {
                        try {
                            bh.consume(calculateFibonacci(fibo));
                        } finally {
                            latch.countDown();
                        }
                    });
        }

        latch.await();
    }

    public static void main(String[] args) throws Exception {
        Options opt =
                new OptionsBuilder()
                        .include(ScalabilityBenchmark.class.getSimpleName())
                        .warmupIterations(5)
                        .measurementIterations(10)
                        .forks(1)
                        .build();

        new Runner(opt).run();
    }
}
