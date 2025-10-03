
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
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeItemEventListener;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeOffsetService;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.JsonRecords;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Thread)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RecordConsumerBenchmark {

    static String[] TOPICS = {"users", "users2"};

    private static final Logger log = LoggerFactory.getLogger(RecordConsumerBenchmark.class);

    @State(Scope.Thread)
    public static class Json {

        @Param({"1"})
        int partitions;

        @Param({"1"})
        int threads;

        // @Param({"true", "false"})
        boolean preferSingleThread = false;

        // @Param({"100", "200", "500"})
        @Param({"1000"})
        int numOfRecords;

        @Param({"10"})
        int numOfKeys = 500;

        @Param({"1"})
        int subscriptions;

        @Param({"ORDER_BY_PARTITION"})
        String ordering;

        ConsumerRecords<String, JsonNode> consumerRecords;
        FakeItemEventListener listener;
        FakeOffsetService offsetService;
        RecordConsumer<String, JsonNode> recordConsumer;

        AtomicInteger iterations = new AtomicInteger();

        private SubscribedItems subscribedItems;

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            this.listener = new FakeItemEventListener(bh);
            this.offsetService = new FakeOffsetService();
        }

        @Setup(Level.Iteration)
        public void setUp() {
            iterations.set(0);
            // Reuse the listener and offsetService created in setUpTrial
            ConsumerTriggerConfig<String, JsonNode> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "JSON");

            // Configure the RecordMapper.
            RecordMapper<String, JsonNode> recordMapper = BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            subscribedItems = subscriptions(subscriptions);
            this.recordConsumer =
                    RecordConsumer.<String, JsonNode>recordMapper(recordMapper)
                            .subscribedItems(subscribedItems)
                            .enforceCommandMode(false)
                            .eventListener(listener)
                            .offsetService(offsetService)
                            .errorStrategy(config.errorHandlingStrategy())
                            .logger(log)
                            .threads(threads)
                            .ordering(OrderStrategy.valueOf(ordering))
                            .preferSingleThread(preferSingleThread)
                            .build();

            // Generate the test records.
            this.consumerRecords =
                    JsonRecords.consumerRecords(TOPICS, partitions, numOfRecords, numOfKeys);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            System.out.println("Invoke " + iterations);
            recordConsumer.close();
            listener.show();
        }
    }

    @State(Scope.Thread)
    public static class Protobuf {

        @Param({"1"})
        int partitions;

        @Param({"1"})
        int threads;

        // @Param({"true", "false"})
        boolean preferSingleThread = false;

        // @Param({"100", "200", "500"})
        @Param({"1000"})
        int numOfRecords;

        @Param({"10"})
        int numOfKeys = 500;

        @Param({"1"})
        int subscriptions;

        @Param({"ORDER_BY_PARTITION"})
        String ordering;

        ConsumerRecords<String, DynamicMessage> consumerRecords;
        FakeItemEventListener listener;
        FakeOffsetService offsetService;
        RecordConsumer<String, DynamicMessage> recordConsumer;

        AtomicInteger iterations = new AtomicInteger();

        private SubscribedItems subscribedItems;

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            this.listener = new FakeItemEventListener(bh);
            this.offsetService = new FakeOffsetService();
        }

        @Setup(Level.Iteration)
        public void setUp() {
            iterations.set(0);
            // Reuse the listener and offsetService created in setUpTrial
            ConsumerTriggerConfig<String, DynamicMessage> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF");

            // Configure the RecordMapper.
            RecordMapper<String, DynamicMessage> recordMapper =
                    BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            subscribedItems = subscriptions(subscriptions);
            this.recordConsumer =
                    RecordConsumer.<String, DynamicMessage>recordMapper(recordMapper)
                            .subscribedItems(subscribedItems)
                            .enforceCommandMode(false)
                            .eventListener(listener)
                            .offsetService(offsetService)
                            .errorStrategy(config.errorHandlingStrategy())
                            .logger(log)
                            .threads(threads)
                            .ordering(OrderStrategy.valueOf(ordering))
                            .preferSingleThread(preferSingleThread)
                            .build();

            // Generate the test records.
            this.consumerRecords =
                    ProtoRecords.consumerRecords(TOPICS, partitions, numOfRecords, numOfKeys);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            System.out.println("Invoke " + iterations);
            recordConsumer.close();
            listener.show();
        }
    }

    private static SubscribedItems subscriptions(int subscriptions) {
        Map<String, SubscribedItem> items = new HashMap<>();
        for (int i = 0; i < subscriptions; i++) {
            // String key = i == 0 ? String.valueOf(i) : "-" + i;
            String key = String.valueOf(i);
            String input = "users-[key=%s,tag=%s,sonTag=%s]".formatted(key, key, key + "-son");
            items.put(input, Items.subscribedFrom(input, new Object()));
        }
        return SubscribedItems.of(items.values());
    }

    @Benchmark
    public void consumeWithJson(Json json) {
        json.iterations.incrementAndGet();
        json.recordConsumer.consumeRecords(json.consumerRecords);
    }

    @Benchmark
    public void consumeWithProtobuf(Protobuf proto) {
        proto.iterations.incrementAndGet();
        proto.recordConsumer.consumeRecords(proto.consumerRecords);
    }

    public static void main(String[] args) throws Exception {
        Options opt =
                new OptionsBuilder()
                        .warmupIterations(5)
                        .warmupTime(TimeValue.seconds(5))
                        .measurementTime(TimeValue.seconds(10))
                        .measurementIterations(10)
                        .include(RecordConsumerBenchmark.class.getSimpleName())
                        .build();

        new Runner(opt).run();
    }
}
