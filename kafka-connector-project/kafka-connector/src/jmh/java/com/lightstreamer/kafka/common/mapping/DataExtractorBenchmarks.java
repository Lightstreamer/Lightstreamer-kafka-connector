
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
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;

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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class DataExtractorBenchmarks {

    static String[] TOPICS = {"users"};
    int partitions = 1;

    @State(Scope.Thread)
    public static class Protobuf {

        @Param({"1"})
        int partitions;

        @Param({"1000"})
        public int numOfRecords;

        @Param({"10"})
        int numOfKeys = 10;

        DataExtractor<String, DynamicMessage> templateExtractor;
        DataExtractor<String, DynamicMessage> fieldsExtractor;
        List<KafkaRecord<String, DynamicMessage>> records;

        @Setup(Level.Iteration)
        public void setUp()
                throws ExtractionException, JsonMappingException, JsonProcessingException {
            ConsumerTriggerConfig<String, DynamicMessage> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "PROTOBUF");
            templateExtractor =
                    config.itemTemplates().groupExtractors().get(TOPICS[0]).iterator().next();
            fieldsExtractor = config.fieldsExtractor();
            records = ProtoRecords.kafkaRecords(TOPICS, 1, numOfRecords, 10);
        }
    }

    @State(Scope.Thread)
    public static class Json {

        @Param({"1"})
        int partitions;

        @Param({"1000"})
        public int numOfRecords;

        @Param({"10"})
        int numOfKeys = 10;

        DataExtractor<String, JsonNode> templateExtractor;
        DataExtractor<String, JsonNode> fieldsExtractor;
        private List<KafkaRecord<String, JsonNode>> records;

        @Setup(Level.Iteration)
        public void setUp()
                throws ExtractionException, JsonMappingException, JsonProcessingException {
            ConsumerTriggerConfig<String, JsonNode> config =
                    BenchmarksUtils.newConfigurator(TOPICS, "JSON");
            templateExtractor =
                    config.itemTemplates().groupExtractors().get(TOPICS[0]).iterator().next();
            fieldsExtractor = config.fieldsExtractor();
            records =
                    BenchmarksUtils.JsonRecords.kafkaRecords(
                            TOPICS, partitions, numOfRecords, numOfKeys);
            System.out.println("Added " + numOfRecords + " records");
        }
    }

    @Benchmark
    public void measureExtractDataFromTemplateWithProtoBuf(Protobuf proto, Blackhole bh) {
        for (int i = 0; i < proto.numOfRecords; i++) {
            SchemaAndValues data = proto.templateExtractor.extractData(proto.records.get(i));
            bh.consume(data);
        }
    }

    @Benchmark
    public void measureExtractDataFromTemplateWithJson(Json json, Blackhole bh) {
        for (int i = 0; i < json.numOfRecords; i++) {
            SchemaAndValues data = json.templateExtractor.extractData(json.records.get(i));
            bh.consume(data);
        }
    }

    @Benchmark
    public void measureExtractDataFromFieldWithProtoBuf(Protobuf proto, Blackhole bh) {
        for (int i = 0; i < proto.numOfRecords; i++) {
            SchemaAndValues data = proto.fieldsExtractor.extractData(proto.records.get(i));
            bh.consume(data);
        }
    }

    @Benchmark
    public void measureExtractDataFromFieldWithJson(Json json, Blackhole bh) {
        for (int i = 0; i < json.numOfRecords; i++) {
            SchemaAndValues data = json.fieldsExtractor.extractData(json.records.get(i));
            bh.consume(data);
        }
    }

    public static void main(String[] args) throws Exception {
        jmh();
    }

    private static void jmh() throws RunnerException {
        Options opt =
                new OptionsBuilder().include(DataExtractorBenchmarks.class.getSimpleName()).build();

        new Runner(opt).run();
    }
}
