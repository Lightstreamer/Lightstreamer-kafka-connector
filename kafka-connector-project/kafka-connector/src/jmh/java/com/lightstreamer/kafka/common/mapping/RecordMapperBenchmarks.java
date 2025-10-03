
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
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;

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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RecordMapperBenchmarks {

    static String[] TOPICS = {"users"};

    @State(Scope.Thread)
    public static class Protobuf {

        @Param({"1"})
        int partitions;

        // @Param({"1", "100", "1000"})
        @Param({"1000"})
        public int numOfRecords;

        @Param({"10"})
        int numOfKeys = 10;

        @Param({"1"})
        int numOfSubscriptions = 5000;

        public RecordMapper<String, DynamicMessage> mapper;
        private List<KafkaRecord<String, DynamicMessage>> records;

        private SubscribedItems subscribedItems = subscriptions(numOfSubscriptions);
        private MappedRecord mappedRecord;

        @Setup(Level.Iteration)
        public void setUp() throws Exception {
            ConsumerTriggerConfig<String, DynamicMessage> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF");
            this.records =
                    BenchmarksUtils.ProtoRecords.kafkaRecords(
                            TOPICS, partitions, numOfRecords, numOfKeys);

            this.mapper = BenchmarksUtils.newRecordMapper(config);
            this.mappedRecord = mapper.map(records.get(0));
        }
    }

    @State(Scope.Thread)
    public static class Json {

        @Param({"1"})
        int partitions;

        // @Param({"1", "100", "1000"})
        @Param({"1000"})
        public int numOfRecords;

        @Param({"10"})
        int numOfKeys = 10;

        @Param({"1"})
        int numOfSubscriptions = 5000;

        // @Param({"true", "false"})
        @Param({"false"})
        boolean regex;

        public RecordMapper<String, JsonNode> mapper;
        private List<KafkaRecord<String, JsonNode>> records;

        private SubscribedItems subscribedItems = subscriptions(numOfSubscriptions);
        private MappedRecord mappedRecord;

        @Setup(Level.Iteration)
        public void setUp() throws Exception {
            ConsumerTriggerConfig<String, JsonNode> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "JSON");
            this.records =
                    BenchmarksUtils.JsonRecords.kafkaRecords(
                            TOPICS, partitions, numOfRecords, numOfKeys);

            this.mapper = BenchmarksUtils.newRecordMapper(config);
            this.mappedRecord = mapper.map(records.get(0));
        }
    }

    private static SubscribedItems subscriptions(int subscriptions) {
        ConcurrentHashMap<String, SubscribedItem> items = new ConcurrentHashMap<>();
        for (int i = 0; i < subscriptions; i++) {
            String key = i == 0 ? String.valueOf(i) : "-" + i;
            String input = "users-[key=%s,tag=%s,sonTag=%s]".formatted(key, key, key + "-son");
            items.put(input, Items.subscribedFrom(input, new Object()));
        }
        return SubscribedItems.of(items.values());
    }

    @Benchmark
    public void measureMapWithJson(Json json, Blackhole bh) throws ExtractionException {
        for (int i = 0; i < json.numOfRecords; i++) {
            MappedRecord map = json.mapper.map(json.records.get(i));
            bh.consume(map);
        }
    }

    @Benchmark
    public void measureMapWithProtobuf(Protobuf proto, Blackhole bh) throws ExtractionException {
        for (int i = 0; i < proto.numOfRecords; i++) {
            MappedRecord map = proto.mapper.map(proto.records.get(i));
            bh.consume(map);
        }
    }

    // @Benchmark
    // public void measureMapOriginal(Blackhole bh) throws ExtractionException {
    //     for (int i = 0; i < numOfRecords; i++) {
    //         MappedRecord map = mapper.mapOriginal(records.get(i));
    //         bh.consume(map);
    //         // System.out.println(map);
    //     }
    // }

    // @Benchmark
    public void measureMapAndFilterWithJson(Json plan, Blackhole bh) throws ExtractionException {
        for (int i = 0; i < plan.numOfRecords; i++) {
            MappedRecord map = plan.mapper.map(plan.records.get(i));
            Map<String, String> filtered = map.fieldsMap();
            bh.consume(filtered);
        }
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
        Options opt =
                new OptionsBuilder()
                        .warmupIterations(5)
                        .warmupTime(TimeValue.seconds(5))
                        .measurementTime(TimeValue.seconds(10))
                        .measurementIterations(10)
                        .include(RecordMapperBenchmarks.class.getSimpleName())
                        .build();

        new Runner(opt).run();
    }
}
