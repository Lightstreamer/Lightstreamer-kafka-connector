
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
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeEventListener;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeDeserializers;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageDeserializers;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

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

        @Param({"100"})
        int numOfSubscriptions = 5000;

        // @Param({ "50000" })
        int numOfKeys = numOfSubscriptions;

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 3;

        private RecordMapper<String, DynamicMessage> mapper;
        private List<KafkaRecord<String, DynamicMessage>> records;
        private SubscribedItems subscribedItems;
        private MappedRecord mappedRecord;

        private KafkaRecord<String, DynamicMessage> record;

        @Setup(Level.Iteration)
        public void setUp(Blackhole bh) throws Exception {
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF", numOfTemplateParams);

            @SuppressWarnings("unchecked")
            Config<String, DynamicMessage> config =
                    (Config<String, DynamicMessage>) configurator.consumerConfig();
            this.subscribedItems =
                    BenchmarksUtils.subscriptions(
                            numOfSubscriptions, new FakeEventListener(bh), numOfTemplateParams);
            DeserializerPair<String, DynamicMessage> deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            DynamicMessageDeserializers.ValueDeserializer(
                                    configurator.getConfig()));

            this.records =
                    KafkaRecord.listFromEager(
                            BenchmarksUtils.ProtoRecords.pollRecords(
                                    TOPICS, partitions, numOfRecords, numOfKeys),
                            deserializerPair);

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

        @Param({"100"})
        int numOfSubscriptions = 5000;

        // @Param({ "50000" })
        int numOfKeys = numOfSubscriptions;

        @Param({"1", "2", "3"})
        int numOfTemplateParams;

        // @Param({"true", "false"})
        @Param({"false"})
        boolean regex;

        private RecordMapper<String, JsonNode> mapper;
        private List<KafkaRecord<String, JsonNode>> records;
        private KafkaRecord<String, JsonNode> record;
        private SubscribedItems subscribedItems;
        private MappedRecord mappedRecord;

        @Setup(Level.Iteration)
        public void setUp(Blackhole bh) throws Exception {
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, "JSON", numOfTemplateParams);

            @SuppressWarnings("unchecked")
            Config<String, JsonNode> config =
                    (Config<String, JsonNode>) configurator.consumerConfig();
            this.subscribedItems =
                    BenchmarksUtils.subscriptions(
                            numOfSubscriptions, new FakeEventListener(bh), numOfTemplateParams);
            DeserializerPair<String, JsonNode> deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            JsonNodeDeserializers.ValueDeserializer(configurator.getConfig()));

            this.records =
                    KafkaRecord.listFromEager(
                            BenchmarksUtils.JsonRecords.pollRecords(
                                    TOPICS, partitions, numOfRecords, numOfKeys),
                            deserializerPair);

            this.mapper = BenchmarksUtils.newRecordMapper(config);
            this.record = records.get(0);
            this.mappedRecord = mapper.map(record);
        }
    }

    /**
     * Benchmarks the performance of mapping a Kafka record to a {@link MappedRecord} using JSON
     * mapping.
     *
     * <p>This benchmark measures the execution time of the {@link RecordMapper#map(Record)} method
     * when applied to JSON records. The mapped result is consumed by the blackhole to prevent dead
     * code elimination and ensure accurate performance measurements.
     *
     * @param json the JSON benchmark state containing the mapper and record to be mapped
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     * @throws ExtractionException if an error occurs during the mapping process
     */
    @Benchmark
    public void measureMapWithJson(Json json, Blackhole bh) throws ExtractionException {
        MappedRecord map = json.mapper.map(json.record);
        bh.consume(map);
    }

    /**
     * Benchmarks the performance of mapping a Kafka record to a {@link MappedRecord} using Protobuf
     * mapping.
     *
     * <p>This benchmark measures the execution time of the {@link RecordMapper#map(Record)} method
     * when applied to Protobuf records. The mapped result is consumed by the blackhole to prevent
     * dead code elimination and ensure accurate performance measurements.
     *
     * @param proto the Protobuf benchmark state containing the mapper and record to be mapped
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     * @throws ExtractionException if an error occurs during the mapping process
     */
    @Benchmark
    public void measureMapWithProtobuf(Protobuf proto, Blackhole bh) throws ExtractionException {
        MappedRecord map = proto.mapper.map(proto.record);
        bh.consume(map);
    }

    /**
     * Benchmarks the combined performance of mapping and field extraction for JSON records.
     *
     * <p>This benchmark measures the combined execution time of:
     *
     * <ul>
     *   <li>Mapping a raw Kafka record using the configured record mapper
     *   <li>Extracting the fields map from the mapped record
     * </ul>
     *
     * The extracted fields map is consumed by the blackhole to prevent dead code elimination and
     * ensure accurate performance measurements.
     *
     * @param json the JSON benchmark state containing the mapper and test records
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     * @throws ExtractionException if an error occurs during record mapping or field extraction
     */
    @Benchmark
    public void measureMapAndFieldsMapWithJson(Json json, Blackhole bh) throws ExtractionException {
        MappedRecord map = json.mapper.map(json.records.get(0));
        Map<String, String> filtered = map.fieldsMap();
        bh.consume(filtered);
    }

    /**
     * Benchmarks the combined performance of mapping and field extraction for Protobuf records.
     *
     * <p>This benchmark measures the combined execution time of:
     *
     * <ul>
     *   <li>Mapping a raw Kafka record using the configured record mapper
     *   <li>Extracting the fields map from the mapped record
     * </ul>
     *
     * The extracted fields map is consumed by the blackhole to prevent dead code elimination and
     * ensure accurate performance measurements.
     *
     * @param proto the Protobuf benchmark state containing the mapper and test records
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     * @throws ExtractionException if an error occurs during record mapping or field extraction
     */
    @Benchmark
    public void measureMapAndFieldsMapWithProtobuf(Protobuf proto, Blackhole bh)
            throws ExtractionException {
        MappedRecord map = proto.mapper.map(proto.records.get(0));
        Map<String, String> filtered = map.fieldsMap();
        bh.consume(filtered);
    }

    /**
     * Benchmarks the routing performance of a mapped record with JSON data.
     *
     * <p>This benchmark measures the time taken to route a record to a set of subscribed items
     * using the JSON routing strategy. The routing operation determines which subscribed items
     * should receive the data based on the record's content. The routed items set is consumed by
     * the blackhole to prevent dead code elimination and ensure accurate performance measurements.
     *
     * @param json the JSON benchmark state containing the mapped record and subscribed items
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public void measureRouteWithJson(Json json, Blackhole bh) {
        Set<SubscribedItem> routed = json.mappedRecord.route(json.subscribedItems);
        bh.consume(routed);
    }

    /**
     * Benchmarks the routing performance of a mapped record with Protobuf data.
     *
     * <p>This benchmark measures the time required to route a single Protobuf-mapped record to the
     * appropriate subscribed items based on their subscription criteria. The routing operation
     * determines which subscribed items should receive the data. The routed items set is consumed
     * by the blackhole to prevent dead code elimination and ensure accurate performance
     * measurements.
     *
     * @param proto the Protobuf benchmark state containing the mapped record and subscribed items
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public void measureRouteWithProtobuf(Protobuf proto, Blackhole bh) {
        Set<SubscribedItem> routed = proto.mappedRecord.route(proto.subscribedItems);
        bh.consume(routed);
    }
}
