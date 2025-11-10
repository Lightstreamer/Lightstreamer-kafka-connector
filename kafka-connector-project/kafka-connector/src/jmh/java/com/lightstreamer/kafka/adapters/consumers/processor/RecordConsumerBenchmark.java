
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
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeItemEventListener;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeOffsetService;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.JsonRecords;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.records.KafkaRecords;

import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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

        @Param({"100"}) // "50000", "100000"})
        int numOfSubscriptions = 5000;

        // @Param({ "50000" })
        int numOfKeys = numOfSubscriptions;

        @Param({"ORDER_BY_PARTITION"})
        String ordering;

        ConsumerRecords<Deferred<String>, Deferred<JsonNode>> consumerRecords;
        FakeItemEventListener listener;
        FakeOffsetService offsetService;
        RecordConsumer<String, JsonNode> recordConsumer;

        private SubscribedItems subscribedItems;

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            this.listener = new FakeItemEventListener(bh);
            this.offsetService = new FakeOffsetService();
        }

        @Setup(Level.Iteration)
        public void setUp() {
            // Reuse the listener and offsetService created in setUpTrial
            Config<String, JsonNode> config =
                    (Config<String, JsonNode>)
                            BenchmarksUtils.newConfigurator(TOPICS, "JSON", 3).consumerConfig();

            // Configure the RecordMapper.
            RecordMapper<String, JsonNode> recordMapper = BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            subscribedItems = BenchmarksUtils.subscriptions(numOfSubscriptions);
            this.recordConsumer =
                    RecordConsumer.<String, JsonNode>recordMapper(recordMapper)
                            .subscribedItems(subscribedItems)
                            .commandMode(CommandModeStrategy.NONE)
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
            recordConsumer.terminate();
        }
    }

    @State(Scope.Thread)
    public static class Protobuf {

        @Param({"1"})
        int partitions;

        @Param({"1"})
        int threads;

        // @Param({"true", "false"})
        boolean preferSingleThread = true;

        // @Param({"100", "200", "500"})
        @Param({"1000"})
        int numOfRecords;

        @Param({"100"}) // "50000", "100000"})
        int numOfSubscriptions = 5000;

        // @Param({ "50000" })
        int numOfKeys = numOfSubscriptions;

        @Param({"ORDER_BY_PARTITION"})
        String ordering;

        ConsumerRecords<Deferred<String>, Deferred<DynamicMessage>> consumerRecords;
        FakeItemEventListener listener;
        FakeOffsetService offsetService;
        RecordConsumer<String, DynamicMessage> recordConsumer;

        private SubscribedItems subscribedItems;

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            this.listener = new FakeItemEventListener(bh);
            this.offsetService = new FakeOffsetService();
        }

        @Setup(Level.Iteration)
        public void setUp() {
            // Reuse the listener and offsetService created in setUpTrial
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF", 3);
            Config<String, DynamicMessage> config =
                    (Config<String, DynamicMessage>) configurator.consumerConfig();

            // Configure the RecordMapper.
            RecordMapper<String, DynamicMessage> recordMapper =
                    BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            subscribedItems = BenchmarksUtils.subscriptions(numOfSubscriptions);
            this.recordConsumer =
                    RecordConsumer.<String, DynamicMessage>recordMapper(recordMapper)
                            .subscribedItems(subscribedItems)
                            .commandMode(CommandModeStrategy.NONE)
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
            recordConsumer.terminate();
        }
    }

    @Benchmark
    public void consumeWithJson(Json json) {
        json.recordConsumer.consumeRecords(KafkaRecords.from(json.consumerRecords));
    }

    @Benchmark
    public void consumeWithProtobuf(Protobuf proto) {
        proto.recordConsumer.consumeRecords(KafkaRecords.from(proto.consumerRecords));
    }

    public static void main1(String[] args) throws Exception {
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
