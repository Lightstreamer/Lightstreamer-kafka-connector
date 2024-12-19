
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
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class DataExtractorBenchmark {

    static String TOPIC = "users";
    int partitions = 1;

    DataExtractor<String, JsonNode> extractor;
    private List<KafkaRecord<String, JsonNode>> records;

    @Param({"1", "100", "1000"})
    public int numOfRecords;

    @Param({"10"})
    int keys = 10;

    public List<ExtractionExpression> exprs = new ArrayList<>();

    @Setup(Level.Iteration)
    public void setUp() throws ExtractionException, JsonMappingException, JsonProcessingException {
        ConsumerTriggerConfig<String, JsonNode> config = BenchmarksUtils.newConfigurator(TOPIC);
        extractor = config.itemTemplates().groupExtractors().get(TOPIC).iterator().next();
        records = BenchmarksUtils.Records.kafkaRecords(TOPIC, partitions, numOfRecords, keys);
        System.out.println("Added " + numOfRecords + " records");
    }

    @Benchmark
    public void misureExtract(Blackhole bh) {
        for (int i = 0; i < numOfRecords; i++) {
            SchemaAndValues data = extractor.extractData(records.get(i));
            bh.consume(data);
        }
    }

    // @Benchmark
    // public void misureExtract1_0(Blackhole bh) {
    //     for (int i = 0; i < size; i++) {
    //         SchemaAndValues data = extractor.extractDataOld1_0(records.get(0));
    //         bh.consume(data);
    //     }
    // }

    // @Benchmark
    // public void misureExtract1_1(Blackhole bh) {
    //     for (int i = 0; i < size; i++) {
    //         SchemaAndValues data = extractor.extractDataOld1_1(records.get(0));
    //         bh.consume(data);
    //     }
    // }

    // @Benchmark
    // public void misureExtract2_0(Blackhole bh) {
    //     for (int i = 0; i < size; i++) {
    //         SchemaAndValues data = extractor.extractData2_0(records.get(0));
    //         bh.consume(data);
    //     }
    // }

    public static void main(String[] args) throws Exception {
        jmh();
        // test();
    }

    private static void test() throws Exception {
        Blackhole bh =
                new Blackhole(
                        "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        DataExtractorBenchmark benchmark = new DataExtractorBenchmark();
        benchmark.numOfRecords = 100;
        benchmark.setUp();
        benchmark.misureExtract(bh);
    }

    private static void jmh() throws RunnerException {
        Options opt =
                new OptionsBuilder().include(DataExtractorBenchmark.class.getSimpleName()).build();

        new Runner(opt).run();
    }
}
