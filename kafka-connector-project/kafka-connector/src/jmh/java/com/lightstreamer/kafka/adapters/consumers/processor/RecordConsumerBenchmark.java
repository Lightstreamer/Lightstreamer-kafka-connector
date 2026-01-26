
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
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeEventListener;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeOffsetService;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.JsonRecords;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.RawRecord;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeDeserializers;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageDeserializers;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serdes;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RecordConsumerBenchmark {

    static String[] TOPICS = {"users", "users2"};
    //     static String[] TOPICS = {"users"};

    private static final Logger logger = LoggerFactory.getLogger(RecordConsumerBenchmark.class);

    @State(Scope.Thread)
    public static class Json {

        @Param({"1"})
        int partitions;

        @Param({"1"})
        int threads;

        @Param({"3"})
        int numOfTemplateParams;

        boolean preferSingleThread = false;

        // @Param({"100", "200", "500"})
        @Param({"1000"})
        int numOfRecords;

        @Param({"100"}) // "50000", "100000"})
        int numOfSubscriptions = 5000;

        int numOfKeys = numOfSubscriptions;

        @Param({"ORDER_BY_PARTITION"})
        String ordering = "ORDER_BY_PARTITION";

        private ConsumerRecords<byte[], byte[]> consumerRecords;
        private List<RawRecord> rawRecords;
        private List<KafkaRecord<String, JsonNode>> deferredKafkaRecords;
        private List<KafkaRecord<String, JsonNode>> eagerKafkaRecords;

        private FakeEventListener listener;
        private FakeOffsetService offsetService;
        private RecordConsumer<String, JsonNode> recordConsumer;

        private SubscribedItems subscribedItems;

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            this.listener = new FakeEventListener(bh);
            this.offsetService = new FakeOffsetService();
        }

        @Setup(Level.Iteration)
        public void setUp() {
            ConnectorConfigurator configurator = BenchmarksUtils.newConfigurator(TOPICS, "JSON", 3);
            @SuppressWarnings("unchecked")
            Config<String, JsonNode> config =
                    (Config<String, JsonNode>) configurator.consumerConfig();

            // Configure the RecordMapper.
            RecordMapper<String, JsonNode> recordMapper = BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            this.subscribedItems =
                    BenchmarksUtils.subscriptions(
                            numOfSubscriptions, listener, numOfTemplateParams);
            this.recordConsumer =
                    RecordConsumer.<String, JsonNode>recordMapper(recordMapper)
                            .subscribedItems(subscribedItems)
                            .commandMode(CommandModeStrategy.NONE)
                            .eventListener(listener)
                            .offsetService(offsetService)
                            .errorStrategy(config.errorHandlingStrategy())
                            .logger(logger)
                            .threads(threads)
                            .ordering(OrderStrategy.valueOf(ordering))
                            .preferSingleThread(preferSingleThread)
                            .build();

            DeserializerPair<String, JsonNode> deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            JsonNodeDeserializers.ValueDeserializer(configurator.getConfig()));

            // Generate the test records.
            this.rawRecords = JsonRecords.rawRecords(TOPICS, partitions, numOfRecords, numOfKeys);
            this.consumerRecords =
                    BenchmarksUtils.pollRecordsFromRaw(TOPICS, partitions, rawRecords);
            this.deferredKafkaRecords =
                    KafkaRecord.listFromDeferred(consumerRecords, deserializerPair);
            this.eagerKafkaRecords = KafkaRecord.listFromEager(consumerRecords, deserializerPair);
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

        @Param({"3"})
        int numOfTemplateParams;

        // @Param({"true", "false"})
        boolean preferSingleThread = true;

        // @Param({"100", "200", "500"})
        @Param({"1000"})
        int numOfRecords;

        @Param({"100"}) // "50000", "100000"})
        int numOfSubscriptions = 5000;

        int numOfKeys = numOfSubscriptions;

        @Param({"ORDER_BY_PARTITION"})
        String ordering = "ORDER_BY_PARTITION";

        private ConsumerRecords<byte[], byte[]> consumerRecords;
        private List<RawRecord> rawRecords;
        private List<KafkaRecord<String, DynamicMessage>> deferredKafkaRecords;
        private List<KafkaRecord<String, DynamicMessage>> eagerKafkaRecords;

        private FakeEventListener listener;
        private FakeOffsetService offsetService;
        private RecordConsumer<String, DynamicMessage> recordConsumer;

        private SubscribedItems subscribedItems;

        private DeserializerPair<String, DynamicMessage> deserializerPair;

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            this.listener = new FakeEventListener(bh);
            this.offsetService = new FakeOffsetService();
        }

        @Setup(Level.Iteration)
        public void setUp() {
            // Reuse the listener and offsetService created in setUpTrial
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF", numOfTemplateParams);
            @SuppressWarnings("unchecked")
            Config<String, DynamicMessage> config =
                    (Config<String, DynamicMessage>) configurator.consumerConfig();

            // Configure the RecordMapper.
            RecordMapper<String, DynamicMessage> recordMapper =
                    BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            this.subscribedItems =
                    BenchmarksUtils.subscriptions(
                            numOfSubscriptions, listener, numOfTemplateParams);
            this.recordConsumer =
                    RecordConsumer.<String, DynamicMessage>recordMapper(recordMapper)
                            .subscribedItems(subscribedItems)
                            .commandMode(CommandModeStrategy.NONE)
                            .eventListener(listener)
                            .offsetService(offsetService)
                            .errorStrategy(config.errorHandlingStrategy())
                            .logger(logger)
                            .threads(threads)
                            .ordering(OrderStrategy.valueOf(ordering))
                            .preferSingleThread(preferSingleThread)
                            .build();

            this.deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            DynamicMessageDeserializers.ValueDeserializer(
                                    configurator.getConfig()));

            // Generate the test records.
            this.rawRecords = ProtoRecords.rawRecords(TOPICS, partitions, numOfRecords, numOfKeys);
            this.consumerRecords =
                    BenchmarksUtils.pollRecordsFromRaw(TOPICS, partitions, rawRecords);
            this.deferredKafkaRecords =
                    KafkaRecord.listFromDeferred(consumerRecords, deserializerPair);
            this.eagerKafkaRecords = KafkaRecord.listFromEager(consumerRecords, deserializerPair);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            recordConsumer.terminate();
            listener.show();
        }
    }

    /**
     * Benchmarks end-to-end consumption and processing of JSON-formatted Kafka records. Measures
     * the complete workflow from record consumption to subscriber notification.
     */
    @Benchmark
    public void consumeWithJsonDeferred(Json json) {
        json.recordConsumer.consumeRecords(json.deferredKafkaRecords);
    }

    @Benchmark
    public void consumeWithJsonEager(Json json) {
        json.recordConsumer.consumeRecords(json.eagerKafkaRecords);
    }

    /**
     * Benchmarks end-to-end consumption and processing of Protobuf-formatted Kafka records.
     * Measures the complete workflow from record consumption to subscriber notification.
     *
     * @return
     */
    @Benchmark
    public void consumeWithProtobufDeferred(Protobuf proto) {
        proto.recordConsumer.consumeRecords(proto.deferredKafkaRecords);
    }

    @Benchmark
    public void consumeWithProtobufEager(Protobuf proto) {
        proto.recordConsumer.consumeRecords(proto.eagerKafkaRecords);
    }

    @Benchmark
    public void pollWithProtobufEager(Protobuf proto) {
        ConsumerRecords<byte[], byte[]> consumerRecords =
                BenchmarksUtils.pollRecordsFromRaw(TOPICS, proto.partitions, proto.rawRecords);
        proto.recordConsumer.consumeRecords(
                KafkaRecord.listFromEager(consumerRecords, proto.deserializerPair));
    }

    @Benchmark
    public void pollWithProtobufDeferred(Protobuf proto) {
        ConsumerRecords<byte[], byte[]> consumerRecords =
                BenchmarksUtils.pollRecordsFromRaw(TOPICS, proto.partitions, proto.rawRecords);
        proto.recordConsumer.consumeRecords(
                KafkaRecord.listFromDeferred(consumerRecords, proto.deserializerPair));
    }

    public static void main(String[] args) throws Exception {
        RecordConsumerBenchmark benchmark = new RecordConsumerBenchmark();
        Json jsonState = new Json();
        jsonState.partitions = 1;
        jsonState.threads = 1;
        jsonState.numOfRecords = 1;
        jsonState.numOfSubscriptions = jsonState.numOfKeys = jsonState.numOfSubscriptions;
        jsonState.numOfTemplateParams = 1;
        Blackhole blackhole =
                new Blackhole(
                        "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        jsonState.setUpTrial(blackhole);
        jsonState.setUp();
        benchmark.consumeWithJsonDeferred(jsonState);
        jsonState.tearDown();
    }

    public static void main1(String[] args) throws Exception {
        // RecordConsumerBenchmark benchmark = new RecordConsumerBenchmark();
        // Protobuf protoBufState = new Protobuf();
        // protoBufState.partitions = 2;
        // protoBufState.threads = 2;
        // protoBufState.numOfRecords = 10000;
        // protoBufState.numOfSubscriptions = 1000;
        // protoBufState.numOfKeys = 10000; // protoBufState.numOfSubscriptions;
        // protoBufState.numOfTemplateParams = 3;
        // Blackhole blackhole =
        //         new Blackhole(
        //                 "Today's password is swordfish. I understand instantiating Blackholes
        // directly is dangerous.");
        // protoBufState.setUpTrial(blackhole);
        // protoBufState.setUp();
        // while (true) {
        //     benchmark.consumeWithProtobuf(protoBufState);
        // }

        // benchmark.consumeWithProtobufEager(protoBufState);
        // benchmark.consumeWithProtobufReady(protoBufState);
        // benchmark.consumeWithProtobufDeferred(protoBufState);
        // System.out.println(protoBufState.valueDeserializer.count.get());

        // protoBufState.tearDown();
    }
}
