
/*
 * Copyright (C) 2014 Lightstreamer Srl
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

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.Records;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.RecordProcessor;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.DefaultRecordProcessor;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS) // Risultati in millisecondi
public class HashedKeyedProcessorBenchmark {

    static String TOPIC = "users";

    private int partitions = 1;

    @Param({"4"})
    private int threadCount;

    @Param({"1500"}) // Numero totale di task
    private int taskCount;

    @Param({"1", "2", "4"})
    // @Param("1")
    private int keySize;

    public enum Processor {
        HASHED,
        MULTIPLEXER;
    }

    @Param({
        "MULTIPLEXER",
        // "HASHED",
    })
    private Processor processor;

    private TaskExecutor<String> executor;
    private int fibo = 30;
    private List<Integer> fibos;
    private volatile int counter;

    private List<ConsumerRecord<String, JsonNode>> consumerRecords;
    BenchmarksUtils.FakeItemEventListener listener;

    private RecordProcessor<String, JsonNode> recordProcessor;

    @Setup(Level.Iteration)
    public void setUp(Blackhole bh) {
        executor =
                switch (processor) {
                    case HASHED -> new HashedKeyedEventProcessor<>(threadCount);
                    case MULTIPLEXER -> new Multiplexer<>(getPool(), false);
                };
        fibos = Collections.nCopies(taskCount, fibo);

        RecordMapper<String, JsonNode> recordMapper =
                newRecordMapper(BenchmarksUtils.newConfigurator(TOPIC));

        listener = new BenchmarksUtils.FakeItemEventListener(bh);
        this.recordProcessor =
                new DefaultRecordProcessor<>(recordMapper, subscriptions(20), listener);
        // Generate the test records.
        this.consumerRecords =
                RecordConsumerSupport.flatRecords(
                        Records.consumerRecords(TOPIC, partitions, fibo, keySize));
    }

    private static RecordMapper<String, JsonNode> newRecordMapper(
            ConsumerTriggerConfig<String, JsonNode> config) {
        return RecordMapper.<String, JsonNode>builder()
                .withTemplateExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    private Collection<SubscribedItem> subscriptions(int subscriptions) {
        ConcurrentHashMap<String, SubscribedItem> items = new ConcurrentHashMap<>();
        for (int i = 0; i < subscriptions; i++) {
            // String key = String.valueOf(new SecureRandom().nextInt(0, keySize));
            String key = i == 0 ? String.valueOf(i) : "-" + i;
            String input = newItem(key, key, key + "-son");
            items.put(input, Items.subscribedFrom(input, new Object()));
        }
        return items.values();
    }

    private static String newItem(String key, String tag, String sonTag) {
        return "users-[key=%s,tag=%s,sonTag=%s]".formatted(key, tag, sonTag);
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws InterruptedException {
        executor.shutdown();
        this.listener.show();
    }

    static long calculateFibonacci(int n) {
        if (n <= 1) return n;
        return calculateFibonacci(n - 1) + calculateFibonacci(n - 2);
    }

    @Benchmark
    public void testScalabilityWithKey(Blackhole bh) throws InterruptedException {
        // AtomicInteger i = new AtomicInteger();
        // executor.executeBatch(
        //         fibos,
        //         e -> i.incrementAndGet() % keySize,
        //         (key, event) -> bh.consume(calculateFibonacci(event)));
        executor.executeBatch(
                consumerRecords, e -> e.key(), (key, record) -> recordProcessor.process(record));
    }

    @Benchmark
    public void testScalabilityNoKey(Blackhole bh) throws InterruptedException {
        // executor.executeBatch(
        //         fibos, e -> null, (key, event) -> bh.consume(calculateFibonacci(event)));
        executor.executeBatch(
                consumerRecords,
                e -> null,
                (key, record) -> {
                    recordProcessor.process(record);
                });
    }

    public static void main(String[] args) throws Exception {
        // test();
        jmh();
    }

    static ExecutorService getPool() {
        AtomicInteger count = new AtomicInteger();
        return Executors.newFixedThreadPool(
                4, r -> new Thread(r, "MultiPlexer-" + count.incrementAndGet()));
    }

    private static void test() throws Exception {
        Blackhole bh =
                new Blackhole(
                        "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        HashedKeyedProcessorBenchmark benchMark = new HashedKeyedProcessorBenchmark();
        benchMark.fibo = 100;
        benchMark.keySize = 2;
        benchMark.processor = Processor.MULTIPLEXER;
        benchMark.setUp(bh);
        benchMark.testScalabilityNoKey(bh);
        benchMark.tearDown();
    }

    private static void jmh() throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(HashedKeyedProcessorBenchmark.class.getSimpleName())
                        .warmupIterations(2)
                        .warmupTime(TimeValue.seconds(5))
                        .measurementIterations(10)
                        .measurementTime(TimeValue.seconds(10))
                        .forks(1)
                        .build();

        new Runner(opt).run();
    }
}
