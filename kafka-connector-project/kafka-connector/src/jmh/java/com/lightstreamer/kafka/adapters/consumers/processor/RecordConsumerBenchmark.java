
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

import static com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageDeserializers.ValueDeserializer;

import static org.apache.kafka.common.serialization.Serdes.String;

import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeEventListener;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeOffsetService;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.PriceInfoRecords;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.offsets.BenchmarkOffsets;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumer.OrderStrategy;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.DeserializationTiming;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.RecordDeserializationMode;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.common.records.RecordBatch;

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
    //     static String[] TOPICS = {"users"};

    private static final Logger logger = LoggerFactory.getLogger("Benchmark");

    @State(Scope.Thread)
    public static class Plan<V> {

        @Param({"JSON", "PROTOBUF"})
        String type;

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

        @Param({"DEFERRED", "EAGER"})
        String deserializationTiming;

        private FakeEventListener listener;
        private FakeOffsetService offsetService;
        private SubscribedItems subscribedItems;
        private RecordConsumer<String, V> recordConsumer;

        private ConsumerRecords<byte[], byte[]> consumerRecords;
        private RecordDeserializationMode<String, V> deserializationMode;
        private RecordBatch<String, V> batch;

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            this.listener = new FakeEventListener(bh);
            this.offsetService = new FakeOffsetService();
        }

        @Setup(Level.Iteration)
        public void setUp() {
            // Reuse the listener and offsetService created in setUpTrial
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, type, numOfTemplateParams);
            @SuppressWarnings("unchecked")
            Config<String, V> config = (Config<String, V>) configurator.consumerConfig();

            // Configure the RecordMapper.
            RecordMapper<String, V> recordMapper = BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            this.subscribedItems =
                    BenchmarksUtils.subscriptions(
                            numOfSubscriptions, listener, numOfTemplateParams);
            this.recordConsumer =
                    RecordConsumer.recordMapper(recordMapper)
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

            // Generate the test records.
            var rawRecords = ProtoRecords.rawRecords(TOPICS, partitions, numOfRecords, numOfKeys);
            this.consumerRecords =
                    BenchmarksUtils.pollRecordsFromRaw(TOPICS, partitions, rawRecords);
            DeserializerPair<String, V> deserializerPair =
                    new DeserializerPair<>(
                            String().deserializer(),
                            BenchmarksUtils.valueDeserializer(type, configurator.getConfig()));
            this.deserializationMode =
                    RecordDeserializationMode.forTiming(
                            DeserializationTiming.valueOf(deserializationTiming), deserializerPair);
            this.batch = this.deserializationMode.toBatch(consumerRecords, true);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            recordConsumer.close();
            listener.show();
        }
    }

    @State(Scope.Thread)
    public static class PriceInfoState {

        static String TOPIC = "ltest";

        static String[] TOPICS = {TOPIC};

        @Param({"1"})
        int threads;

        @Param({"1000"})
        int numOfRecords;

        @Param({"100"})
        int numOfSubscriptions = 5000;

        @Param({"DEFERRED", "EAGER"})
        String deserializationTiming;

        private PriceInfoRecords priceInfoRecords;
        private FakeEventListener listener;
        private OffsetService offsetService;
        private SubscribedItems subscribedItems;

        private RecordConsumer<String, DynamicMessage> recordConsumer;
        private ConsumerRecords<byte[], byte[]> consumerRecords;
        private RecordDeserializationMode<String, DynamicMessage> deserializationMode;

        private static final String RELATIVE_DESCRIPTOR_PATH =
                "build/generated/sources/proto/jmh/descriptor_set.desc";

        @Setup(Level.Trial)
        public void setUpTrial(Blackhole bh) {
            setupTrial(bh, "./" + RELATIVE_DESCRIPTOR_PATH);
        }

        void setupTrial(Blackhole bh, String descriptorPath) {
            this.priceInfoRecords = new PriceInfoRecords(TOPIC, descriptorPath);
            this.listener = new FakeEventListener(bh);
            this.offsetService = new BenchmarkOffsets.BenchmarkOffsetServiceImpl(logger);
        }

        @Setup(Level.Iteration)
        public void setUp() {
            ConnectorConfigurator configurator = priceInfoRecords.newConfigurator();
            @SuppressWarnings("unchecked")
            Config<String, DynamicMessage> config =
                    (Config<String, DynamicMessage>) configurator.consumerConfig();

            // Configure the RecordMapper.
            RecordMapper<String, DynamicMessage> recordMapper =
                    BenchmarksUtils.newRecordMapper(config);

            // Make the RecordConsumer.
            this.subscribedItems = priceInfoRecords.subscriptions(numOfSubscriptions, listener);

            this.recordConsumer =
                    RecordConsumer.<String, DynamicMessage>recordMapper(recordMapper)
                            .subscribedItems(subscribedItems)
                            .commandMode(CommandModeStrategy.NONE)
                            .eventListener(listener)
                            .offsetService(offsetService)
                            .errorStrategy(config.errorHandlingStrategy())
                            .logger(logger)
                            .threads(threads)
                            .ordering(OrderStrategy.ORDER_BY_KEY)
                            .preferSingleThread(false)
                            .build();

            var deserializerPair =
                    new DeserializerPair<>(
                            String().deserializer(), ValueDeserializer(configurator.getConfig()));

            // Generate the test records.
            var rawRecords = priceInfoRecords.rawRecords(numOfRecords);
            this.consumerRecords = BenchmarksUtils.pollRecordsFromRaw(TOPICS, 1, rawRecords);
            this.deserializationMode =
                    RecordDeserializationMode.forTiming(
                            DeserializationTiming.valueOf(deserializationTiming), deserializerPair);
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            recordConsumer.close();
            listener.show();
        }
    }

    /**
     * Benchmarks the consumption of a pre-deserialized batch of records.
     *
     * <p>This benchmark measures the time taken to consume and process a batch of records that has
     * already been deserialized, focusing on the record processing overhead.
     *
     * @param <V> the type of values in the records being consumed
     * @param plan the benchmark plan containing the record consumer and pre-deserialized batch
     */
    @Benchmark
    public <V> void consume(Plan<V> plan) {
        plan.recordConsumer.consumeBatch(plan.batch);
        plan.batch.join();
    }

    /**
     * Benchmarks the complete poll-to-consume cycle including deserialization.
     *
     * <p>This benchmark measures the time taken to deserialize raw consumer records into a batch
     * and then consume and process that batch, simulating a full polling cycle.
     *
     * @param <V> the type of values in the records being consumed
     * @param plan the benchmark plan containing the deserialization mode, raw records, and record
     *     consumer
     */
    @Benchmark
    public <V> void poll(Plan<V> plan) {
        RecordBatch<String, V> batch = plan.deserializationMode.toBatch(plan.consumerRecords, true);
        plan.recordConsumer.consumeBatch(batch);
        batch.join();
    }

    /**
     * Benchmarks the complete poll-to-consume cycle for PriceInfo protobuf records.
     *
     * <p>This benchmark measures the time taken to deserialize raw PriceInfo consumer records into
     * a batch and then consume and process that batch, focusing on protobuf-specific
     * deserialization and processing performance.
     *
     * @param priceInfoState the benchmark state containing the deserialization mode, raw records,
     *     and record consumer
     */
    @Benchmark
    public void pollPriceInfo(PriceInfoState priceInfoState) {
        RecordBatch<String, DynamicMessage> batch =
                priceInfoState.deserializationMode.toBatch(priceInfoState.consumerRecords, true);
        priceInfoState.recordConsumer.consumeBatch(batch);
        batch.join();
    }
}
