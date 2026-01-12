
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

package com.lightstreamer.kafka.common.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;

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
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RecordMapperBenchmarks {

    static String[] TOPICS = {"users"};

    @State(Scope.Thread)
    public static class Protobuf {

        @Param({"1"})
        int partitions;

        public int numOfRecords = 1;

        @Param({"100"}) // "50000", "100000"})
        int numOfSubscriptions = 5000;

        // @Param({ "50000" })
        int numOfKeys = numOfSubscriptions;

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 3;

        public RecordMapper<String, DynamicMessage> mapper;
        private List<KafkaRecord<String, DynamicMessage>> records;

        private SubscribedItems subscribedItems = BenchmarksUtils.subscriptions(numOfSubscriptions);
        private MappedRecord mappedRecord;

        private KafkaRecord<String, DynamicMessage> record;

        @Setup(Level.Iteration)
        public void setUp() throws Exception {
            @SuppressWarnings("unchecked")
            Config<String, DynamicMessage> config =
                    (Config<String, DynamicMessage>)
                            BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF", numOfTemplateParams)
                                    .consumerConfig();

            this.records =
                    BenchmarksUtils.ProtoRecords.kafkaRecords(
                            TOPICS, partitions, numOfRecords, numOfKeys);

            this.mapper = BenchmarksUtils.newRecordMapper(config);
            this.record = records.get(0);
            this.mappedRecord = mapper.map(record);
        }
    }

    @State(Scope.Thread)
    public static class Json {

        @Param({"1"})
        int partitions;

        public int numOfRecords = 1;

        @Param({"100"}) // "50000", "100000"})
        int numOfSubscriptions = 5000;

        // @Param({ "50000" })
        int numOfKeys = numOfSubscriptions;

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 3;

        // @Param({"true", "false"})
        @Param({"false"})
        boolean regex;

        public RecordMapper<String, JsonNode> mapper;
        private List<KafkaRecord<String, JsonNode>> records;
        private KafkaRecord<String, JsonNode> record;

        private SubscribedItems subscribedItems = BenchmarksUtils.subscriptions(numOfSubscriptions);
        private MappedRecord mappedRecord;

        @Setup(Level.Iteration)
        public void setUp() throws Exception {
            @SuppressWarnings("unchecked")
            Config<String, JsonNode> config =
                    (Config<String, JsonNode>)
                            BenchmarksUtils.newConfigurator(TOPICS, "JSON", numOfTemplateParams)
                                    .consumerConfig();

            this.records =
                    BenchmarksUtils.JsonRecords.kafkaRecords(
                            TOPICS, partitions, numOfRecords, numOfKeys);

            this.mapper = BenchmarksUtils.newRecordMapper(config);
            this.record = records.get(0);
            this.mappedRecord = mapper.map(record);
        }
    }

    /**
     * Benchmarks the mapping of JSON-formatted Kafka records to MappedRecord objects. Measures the
     * overhead of the complete record mapping process for JSON data.
     */
    @Benchmark
    public void measureMapWithJson(Json json, Blackhole bh) throws ExtractionException {
        MappedRecord map = json.mapper.map(json.record);
        bh.consume(map);
    }

    /**
     * Benchmarks the mapping of Protobuf-formatted Kafka records to MappedRecord objects. Measures
     * the overhead of the complete record mapping process for Protobuf data.
     */
    @Benchmark
    public void measureMapWithProtobuf(Protobuf proto, Blackhole bh) throws ExtractionException {
        MappedRecord map = proto.mapper.map(proto.record);
        bh.consume(map);
    }

    /**
     * Benchmarks the complete mapping and field extraction workflow for JSON records. Measures the
     * performance of mapping a record and then extracting its fields into a Map.
     */
    @Benchmark
    public void measureMapAndFieldsMapWithJson(Json plan, Blackhole bh) throws ExtractionException {
        MappedRecord map = plan.mapper.map(plan.records.get(0));
        Map<String, String> filtered = map.fieldsMap();
        bh.consume(filtered);
    }

    /**
     * Benchmarks the complete mapping and field extraction workflow for Protobuf records. Measures
     * the performance of mapping a record and then extracting its fields into a Map.
     */
    @Benchmark
    public void measureMapAndFieldsMapWithProtobuf(Protobuf plan, Blackhole bh)
            throws ExtractionException {
        MappedRecord map = plan.mapper.map(plan.records.get(0));
        Map<String, String> filtered = map.fieldsMap();
        bh.consume(filtered);
    }

    @Benchmark
    public void measureRoutWithJson(Json plan, Blackhole bh) {
        Set<SubscribedItem> routed = plan.mappedRecord.route(plan.subscribedItems);
        bh.consume(routed);
    }

    @Benchmark
    public void measureRouteWithProtobuf(Protobuf plan, Blackhole bh) {
        Set<SubscribedItem> routed = plan.mappedRecord.route(plan.subscribedItems);
        bh.consume(routed);
    }

    public static void main(String[] args) throws Exception {
        String profilerOptions =
                String.join(
                        ";",
                        "libPath=/home/gianluca/ls-projects/lightstreamer-kafka-connector-main/async-profiler-4.1-linux-x64/lib/libasyncProfiler.so",
                        "output=flamegraph",
                        "simple=true",
                        "event=cpu",
                        "dir=./profile-results-fixes");
        Options opt =
                new OptionsBuilder()
                        .warmupIterations(5)
                        .warmupTime(TimeValue.seconds(5))
                        .measurementTime(TimeValue.seconds(10))
                        .measurementIterations(10)
                        .addProfiler(AsyncProfiler.class, profilerOptions)
                        .include(RecordMapperBenchmarks.class.getSimpleName())
                        .build();

        new Runner(opt).run();
    }
}
