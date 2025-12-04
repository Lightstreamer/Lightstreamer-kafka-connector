
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
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.JsonRecords;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
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

import java.util.List;
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
        int numOfTemplateParams = 3;

        DataExtractor<String, DynamicMessage> templateExtractor;
        FieldsExtractor<String, DynamicMessage> fieldsExtractor;
        List<KafkaRecord<String, DynamicMessage>> records;

        @Setup(Level.Iteration)
        public void setUp()
                throws ExtractionException, JsonMappingException, JsonProcessingException {
            ConsumerTriggerConfig<String, DynamicMessage> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF", numOfTemplateParams);
            templateExtractor =
                    config.itemTemplates().groupExtractors().get(TOPICS[0]).iterator().next();
            fieldsExtractor = config.fieldsExtractor();
            records = ProtoRecords.kafkaRecords(TOPICS, 1, 1, 10);
        }
    }

    @State(Scope.Thread)
    public static class Json {

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 3;

        DataExtractor<String, JsonNode> templateExtractor;
        FieldsExtractor<String, JsonNode> fieldsExtractor;
        private List<KafkaRecord<String, JsonNode>> records;

        @Setup(Level.Iteration)
        public void setUp()
                throws ExtractionException, JsonMappingException, JsonProcessingException {
            ConsumerTriggerConfig<String, JsonNode> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "JSON", numOfTemplateParams);
            templateExtractor =
                    config.itemTemplates().groupExtractors().get(TOPICS[0]).iterator().next();
            fieldsExtractor = config.fieldsExtractor();
            records = JsonRecords.kafkaRecords(TOPICS, 1, 1, 1);
        }
    }

    // Measure extraction from templates
    @Benchmark
    public void extractAsCanonicalItemProtoBuf(Protobuf proto, Blackhole bh) {
        String data = proto.templateExtractor.extractAsCanonicalItem(proto.records.get(0));
        bh.consume(data);
    }

    @Benchmark
    public void extractAsCanonicalItemJson(Json json, Blackhole bh) {
        String data = json.templateExtractor.extractAsCanonicalItem(json.records.get(0));
        bh.consume(data);
    }

    // Measure extraction from fields (all fields, no template)
    @Benchmark
    public void extractAsMapProtoBuf(Protobuf proto, Blackhole bh) {
        Map<String, String> data = proto.fieldsExtractor.extractMap(proto.records.get(0));
        bh.consume(data);
    }

    @Benchmark
    public void extractAsMapJson(Json json, Blackhole bh) {
        Map<String, String> data = json.fieldsExtractor.extractMap(json.records.get(0));
        bh.consume(data);
    }
}
