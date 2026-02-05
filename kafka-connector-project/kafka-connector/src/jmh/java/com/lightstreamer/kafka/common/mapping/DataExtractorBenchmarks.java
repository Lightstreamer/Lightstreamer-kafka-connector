
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.JsonRecords;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeDeserializers;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageDeserializers;
import com.lightstreamer.kafka.common.mapping.selectors.CanonicalItemExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.common.records.RecordBatch;

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

import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class DataExtractorBenchmarks {

    static String[] TOPICS = {"users"};

    @State(Scope.Thread)
    public static class Protobuf {

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 1;

        CanonicalItemExtractor<String, DynamicMessage> canonicalItemExtractor;
        FieldsExtractor<String, DynamicMessage> fieldsExtractor;
        KafkaRecord<String, DynamicMessage> kafkaRecord;

        @Setup(Level.Iteration)
        public void setUp()
                throws ExtractionException, JsonMappingException, JsonProcessingException {
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF", numOfTemplateParams);
            @SuppressWarnings("unchecked")
            Config<String, DynamicMessage> config =
                    (Config<String, DynamicMessage>) configurator.consumerConfig();

            this.canonicalItemExtractor =
                    config.itemTemplates().groupExtractors().get(TOPICS[0]).iterator().next();
            this.fieldsExtractor = config.fieldsExtractor();
            DeserializerPair<String, DynamicMessage> deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            DynamicMessageDeserializers.ValueDeserializer(
                                    configurator.getConfig()));

            this.kafkaRecord =
                    RecordBatch.batchFromEager(
                                    ProtoRecords.pollRecords(TOPICS, 1, 1, 10), deserializerPair)
                            .getRecords()
                            .get(0);
            // this.kafkaRecords =
            //         KafkaRecord.listFromDeferred(
            //                 ProtoRecords.pollRecords(TOPICS, 1, 1, 10),
            //                 keyDeserializer,
            //                 valueDeserializer);
        }
    }

    @State(Scope.Thread)
    public static class Json {

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 3;

        CanonicalItemExtractor<String, JsonNode> canonicalItemExtractor;
        FieldsExtractor<String, JsonNode> fieldsExtractor;
        private KafkaRecord<String, JsonNode> kafkaRecord;

        @Setup(Level.Iteration)
        public void setUp()
                throws ExtractionException, JsonMappingException, JsonProcessingException {
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, "JSON", numOfTemplateParams);
            @SuppressWarnings("unchecked")
            Config<String, JsonNode> config =
                    (Config<String, JsonNode>) configurator.consumerConfig();

            canonicalItemExtractor =
                    config.itemTemplates().groupExtractors().get(TOPICS[0]).iterator().next();
            fieldsExtractor = config.fieldsExtractor();
            DeserializerPair<String, JsonNode> deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            JsonNodeDeserializers.ValueDeserializer(configurator.getConfig()));

            this.kafkaRecord =
                    RecordBatch.batchFromEager(
                                    JsonRecords.pollRecords(TOPICS, 1, 1, 10), deserializerPair)
                            .getRecords()
                            .get(0);
        }
    }

    /**
     * Benchmarks the extraction of canonical item names from Protobuf-formatted Kafka records.
     *
     * <p>This benchmark measures the performance of converting Protobuf data into canonical string
     * format. The extracted canonical item name is consumed by the blackhole to prevent dead code
     * elimination and ensure accurate performance measurements.
     *
     * @param proto the Protobuf benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    //     @Benchmark
    //     public void extractAsCanonicalItemProtoBuf(Protobuf proto, Blackhole bh) {
    //         String data = proto.canonicalItemExtractor.extractCanonicalItem(proto.kafkaRecord);
    //         bh.consume(data);
    //     }

    /**
     * Benchmarks the extraction of canonical item names from JSON-formatted Kafka records.
     *
     * <p>This benchmark measures the performance of converting JSON data into canonical string
     * format. The extracted canonical item name is consumed by the blackhole to prevent dead code
     * elimination and ensure accurate performance measurements.
     *
     * @param json the JSON benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    //     @Benchmark
    //     public void extractAsCanonicalItemJson(Json json, Blackhole bh) {
    //         String data = json.canonicalItemExtractor.extractCanonicalItem(json.kafkaRecord);
    //         bh.consume(data);
    //     }

    /**
     * Benchmarks extraction of Protobuf records into a new Map structure.
     *
     * <p>This benchmark measures the performance of creating and populating a new Map with
     * extracted field data from Protobuf records. The Map is consumed by the blackhole to prevent
     * dead code elimination and ensure accurate performance measurements.
     *
     * @param proto the Protobuf benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public void extractAsMapProtoBuf(Protobuf proto, Blackhole bh) {
        Map<String, String> data = proto.fieldsExtractor.extractMap(proto.kafkaRecord);
        bh.consume(data);
    }

    /**
     * Benchmarks extraction of JSON records into a new Map structure.
     *
     * <p>This benchmark measures the performance of creating and populating a new Map with
     * extracted field data from JSON records. The Map is consumed by the blackhole to prevent dead
     * code elimination and ensure accurate performance measurements.
     *
     * @param json the JSON benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    //     @Benchmark
    //     public void extractAsMapJson(Json json, Blackhole bh) {
    //         Map<String, String> data = json.fieldsExtractor.extractMap(json.kafkaRecord);
    //         bh.consume(data);
    //     }

    /**
     * Benchmarks extraction of Protobuf records into a pre-existing Map.
     *
     * <p>This benchmark measures the performance of populating an existing Map with extracted field
     * data from Protobuf records. This approach may be more efficient than creating a new Map. The
     * populated Map is consumed by the blackhole to prevent dead code elimination and ensure
     * accurate performance measurements.
     *
     * @param proto the Protobuf benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    //     @Benchmark
    //     public void extractIntoMapProtoBuf(Protobuf proto, Blackhole bh) {
    //         Map<String, String> target = new HashMap<>();
    //         proto.fieldsExtractor.extractIntoMap(proto.kafkaRecord, target);
    //         bh.consume(target);
    //     }

    /**
     * Benchmarks extraction of JSON records into a pre-existing Map.
     *
     * <p>This benchmark measures the performance of populating an existing Map with extracted field
     * data from JSON records. This approach may be more efficient than creating a new Map. The
     * populated Map is consumed by the blackhole to prevent dead code elimination and ensure
     * accurate performance measurements.
     *
     * @param json the JSON benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    //     @Benchmark
    //     public void extractIntoMapJson(Json json, Blackhole bh) {
    //         Map<String, String> target = new HashMap<>();
    //         json.fieldsExtractor.extractIntoMap(json.kafkaRecord, target);
    //         bh.consume(target);
    //     }
}
