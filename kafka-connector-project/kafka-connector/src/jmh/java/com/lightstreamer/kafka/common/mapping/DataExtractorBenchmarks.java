
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
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.ProtoRecords;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.DeserializationTiming;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.RecordDeserializationMode;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.mapping.selectors.CanonicalItemExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.KafkaRecord.DeserializerPair;
import com.lightstreamer.kafka.common.records.RecordBatch;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
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
    public static class Plan<V> {

        @Param({"JSON", "PROTOBUF"})
        String type;

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 1;

        CanonicalItemExtractor<String, V> canonicalItemExtractor;
        FieldsExtractor<String, V> fieldsExtractor;
        KafkaRecord<String, V> kafkaRecord;

        @Setup(Level.Iteration)
        public void setUp()
                throws ExtractionException, JsonMappingException, JsonProcessingException {
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, type, numOfTemplateParams);
            @SuppressWarnings("unchecked")
            Config<String, V> config = (Config<String, V>) configurator.consumerConfig();

            this.canonicalItemExtractor =
                    config.itemTemplates().groupExtractors().get(TOPICS[0]).iterator().next();
            this.fieldsExtractor = config.fieldsExtractor();

            // Generate the test records.
            var rawRecords = ProtoRecords.rawRecords(TOPICS, 1, 1, 1);
            ConsumerRecords<byte[], byte[]> consumerRecords =
                    BenchmarksUtils.pollRecordsFromRaw(TOPICS, 1, rawRecords);
            DeserializerPair<String, V> deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            BenchmarksUtils.valueDeserializer(type, configurator.getConfig()));
            var deserializationMode =
                    RecordDeserializationMode.forTiming(
                            DeserializationTiming.EAGER, deserializerPair);
            RecordBatch<String, V> recordBatch = deserializationMode.toBatch(consumerRecords);
            this.kafkaRecord = recordBatch.getRecords().get(0);
        }
    }

    /**
     * Benchmarks the extraction of canonical item names from Kafka records.
     *
     * <p>This benchmark measures the performance of converting record data into canonical string
     * format.
     *
     * @param plan the benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public <V> void extractAsCanonicalItem(Plan<V> plan, Blackhole bh) {
        String data = plan.canonicalItemExtractor.extractCanonicalItem(plan.kafkaRecord);
        bh.consume(data);
    }

    /**
     * Benchmarks extraction of Kafka records into a new Map structure.
     *
     * <p>This benchmark measures the performance of creating and populating a new Map with
     * extracted field data from Kafka records.
     *
     * @param plan the benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public <V> void extractAsMap(Plan<V> plan, Blackhole bh) {
        Map<String, String> data = plan.fieldsExtractor.extractMap(plan.kafkaRecord);
        bh.consume(data);
    }

    /**
     * Benchmarks extraction of Kafka records into a pre-existing Map.
     *
     * <p>This benchmark measures the performance of populating an existing Map with extracted field
     * data from Kafka records. This approach may be more efficient than creating a new Map.
     *
     * @param plan the benchmark state containing records and extractors
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public <V> void extractIntoMap(Plan<V> plan, Blackhole bh) {
        Map<String, String> target = new HashMap<>();
        plan.fieldsExtractor.extractIntoMap(plan.kafkaRecord, target);
        bh.consume(target);
    }
}
