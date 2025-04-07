
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
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
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

import java.io.Console;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RecordMapperBenchmark {

    static String[] TOPICS = {"users"};
    int partitions = 1;

    public RecordMapper<String, JsonNode> mapper;
    private List<KafkaRecord<String, JsonNode>> records;

    // @Param({"1", "100", "1000"})
    @Param({"1000"})
    public int numOfRecords;

    @Param({"10"})
    int numOfKeys = 10;

    // @Param({"true", "false"})
    @Param({"false"})
    boolean regex;

    int numOfSubscriptions = 5000;
    private Collection<SubscribedItem> subscribedItems = subscriptions(numOfSubscriptions);
    private MappedRecord mappedRecord;

    @Setup(Level.Iteration)
    public void setUp() throws Exception {
        ConsumerTriggerConfig<String, JsonNode> config = BenchmarksUtils.newConfigurator(TOPICS);
        this.mapper = newRecordMapper(config, regex);
        this.records =
                BenchmarksUtils.Records.kafkaRecords(TOPICS, partitions, numOfRecords, numOfKeys);
        this.mappedRecord = mapper.map(records.get(0));
    }

    private static RecordMapper<String, JsonNode> newRecordMapper(
            ConsumerTriggerConfig<String, JsonNode> config, boolean regex) {
        return RecordMapper.<String, JsonNode>builder()
                .withTemplateExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    private Collection<SubscribedItem> subscriptions(int subscriptions) {
        ConcurrentHashMap<String, SubscribedItem> items = new ConcurrentHashMap<>();
        for (int i = 0; i < subscriptions; i++) {
            String key = i == 0 ? String.valueOf(i) : "-" + i;
            String input = newItem(key, key, key + "-son");
            items.put(input, Items.subscribedFrom(input, new Object()));
        }
        return items.values();
    }

    private static String newItem(String key, String tag, String sonTag) {
        return "users-[key=%s,tag=%s,sonTag=%s]".formatted(key, tag, sonTag);
    }

    @Benchmark
    public void measureMap(Blackhole bh) throws ExtractionException {
        for (int i = 0; i < numOfRecords; i++) {
            MappedRecord map = mapper.map(records.get(i));
            bh.consume(map);
            // System.out.println(map);
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
    public void measureMapAndFilter(Blackhole bh) throws ExtractionException {
        for (int i = 0; i < numOfRecords; i++) {
            MappedRecord map = mapper.map(records.get(i));
            Map<String, String> filtered = map.fieldsMap();
            bh.consume(filtered);
        }
    }

    @Benchmark
    public void measureRoute(Blackhole bh) {
        Set<SubscribedItem> routed = mappedRecord.route(subscribedItems);
        bh.consume(routed);
    }

    public static void test() throws Exception {
        Console console = System.console();
        console.readLine("Hit cr to start the test...");

        Blackhole bh =
                new Blackhole(
                        "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
        RecordMapperBenchmark benchmark = new RecordMapperBenchmark();
        //         new Blackhole(
        //                 "Today's password is swordfish. I understand instantiating Blackholes
        benchmark.numOfRecords = 1000;
        benchmark.setUp();
        benchmark.measureMap(bh);
        // // benchmark.misureMapAndFilter(bh);
        // for (int i = 0; i < 1_000; i++) {
        //     benchmark.route(bh);
        // }
        console.readLine("Hit cr to close the test...");
    }

    public static void main(String[] args) throws Exception {
        // test();
        jmh(args);
    }

    private static void jmh(String[] args) throws Exception {
        Options opt =
                new OptionsBuilder().include(RecordMapperBenchmark.class.getSimpleName()).build();

        new Runner(opt).run();
    }
}
