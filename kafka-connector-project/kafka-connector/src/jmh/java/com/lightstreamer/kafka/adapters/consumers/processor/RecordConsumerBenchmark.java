
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

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeItemEventListener;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeOffsetService;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.Records;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Thread)
// @Warmup(iterations =  5, time = 10, timeUnit = TimeUnit.SECONDS)
// @Measurement(iterations = 15, time = 20, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 4, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RecordConsumerBenchmark {

    static final int WARMUP_CYCLES = 100;

    static String TOPIC = "users";

    static Logger log = LoggerFactory.getLogger(RecordConsumerBenchmark.class);

    @Param({"2", "4", "8"})
    // @Param({"4", "16"})
    int threads = 4;

    int partitions = 2;

    // @Param({"true", "false"})
    boolean prefereSingleThread = false;

    // @Param({"100", "200", "500"})
    @Param({"500"})
    int numOfRecords = 40;

    @Param({"2", "4"})
    int numOfKeys = 2;

    // @Param({"20", "200", "2000"})
    int susbcriptions = 5000;

    @Param({"ORDER_BY_PARTITION", "ORDER_BY_KEY"})
    // @Param({"UNORDERED"})
    String ordering;

    ConsumerRecords<String, JsonNode> consumerRecords;
    FakeItemEventListener listener;
    RecordConsumer<String, JsonNode> recordConsumer;

    AtomicInteger iterations = new AtomicInteger();

    @Setup(Level.Iteration)
    public void setUp(Blackhole bh) {
        iterations.set(0);
        this.listener = new FakeItemEventListener(bh);
        ConsumerTriggerConfig<String, JsonNode> config = BenchmarksUtils.newConfigurator(TOPIC);

        // Configure the RecordMapper.
        RecordMapper<String, JsonNode> recordMapper = newRecordMapper(config);

        // Make the RecordConsumer.
        this.recordConsumer =
                RecordConsumer.<String, JsonNode>recordMapper(recordMapper)
                        .subscribedItems(subscriptions(susbcriptions))
                        .eventListener(listener)
                        .offsetService(new FakeOffsetService())
                        .errorStrategy(config.errorHandlingStrategy())
                        .logger(log)
                        .threads(threads)
                        .ordering(OrderStrategy.valueOf(ordering))
                        .preferSingleThread(prefereSingleThread)
                        .build();

        // Generate the test records.
        this.consumerRecords = Records.consumerRecords(TOPIC, partitions, numOfRecords, numOfKeys);
    }

    private static RecordMapper<String, JsonNode> newRecordMapper(
            ConsumerTriggerConfig<String, JsonNode> config) {
        return RecordMapper.<String, JsonNode>builder()
                .withTemplateExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        System.out.println("Invoke " + iterations);
        recordConsumer.close();
        listener.show();
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

    @Benchmark()
    public void consume() {
        iterations.incrementAndGet();
        recordConsumer.consumeRecords(consumerRecords);
    }

    public static void main(String[] args) throws Exception {
        // test();
        jmh();
    }

    public static void test() throws RunnerException, InterruptedException {
        RecordConsumerBenchmark benchMark = new RecordConsumerBenchmark();
        benchMark.threads = 4;
        benchMark.partitions = 2;
        benchMark.ordering = "UNORDERED";
        benchMark.numOfRecords = 500;
        benchMark.susbcriptions = 5000;
        benchMark.numOfKeys = 2;
        benchMark.setUp(
                new Blackhole(
                        "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
        System.out.println("Start running");

        // Multiplexer.ENABLE_DUMP = false;
        // for (int i = 0; i < WARMUP_CYCLES; i++) {
        //     benchMark.recordConsumer.consumeRecords(benchMark.consumerRecords);
        // }

        benchMark.recordConsumer.consumeRecords(benchMark.consumerRecords);
        benchMark.tearDown();
    }

    private static void jmh() throws RunnerException {

        Options opt =
                new OptionsBuilder().include(RecordConsumerBenchmark.class.getSimpleName()).build();

        new Runner(opt).run();
    }
}
