
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

import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils;
import com.lightstreamer.kafka.adapters.consumers.BenchmarksUtils.FakeEventListener;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.DeserializationTiming;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapper.RecordDeserializationMode;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
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
    public static class Plan<V> {

        @Param({"JSON", "PROTOBUF"})
        String type;

        @Param({"100"})
        int numOfSubscriptions = 5000;

        // @Param({ "50000" })
        int numOfKeys = numOfSubscriptions;

        @Param({"1", "2", "3"})
        int numOfTemplateParams = 3;

        private RecordMapper<String, V> mapper;
        private SubscribedItems subscribedItems;
        private MappedRecord mappedRecord;

        private KafkaRecord<String, V> record;

        @Setup(Level.Iteration)
        public void setUp(Blackhole bh) throws Exception {
            ConnectorConfigurator configurator =
                    BenchmarksUtils.newConfigurator(TOPICS, type, numOfTemplateParams);

            @SuppressWarnings("unchecked")
            Config<String, V> config = (Config<String, V>) configurator.consumerConfig();
            this.subscribedItems =
                    BenchmarksUtils.subscriptions(
                            numOfSubscriptions, new FakeEventListener(bh), numOfTemplateParams);

            // Generate the test records.
            ConsumerRecords<byte[], byte[]> consumerRecords =
                    BenchmarksUtils.pollRecords(type, TOPICS, 1, 1, 1);
            DeserializerPair<String, V> deserializerPair =
                    new DeserializerPair<>(
                            Serdes.String().deserializer(),
                            BenchmarksUtils.valueDeserializer(type, configurator.getConfig()));

            var deserializationMode =
                    RecordDeserializationMode.forTiming(
                            DeserializationTiming.EAGER, deserializerPair);
            RecordBatch<String, V> recordBatch = deserializationMode.toBatch(consumerRecords);

            this.mapper = BenchmarksUtils.newRecordMapper(config);
            this.record = recordBatch.getRecords().get(0);
            this.mappedRecord = mapper.map(record);
        }
    }

    /**
     * Benchmarks the performance of mapping a Kafka record to a {@link MappedRecord}.
     *
     * <p>This benchmark measures the execution time of the {@link RecordMapper#map(Record)} method.
     *
     * @param plan the benchmark state containing the mapper and record to be mapped
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public <V> void map(Plan<V> plan, Blackhole bh) {
        MappedRecord map = plan.mapper.map(plan.record);
        bh.consume(map);
    }

    /**
     * Benchmarks the combined performance of mapping and field extraction for Kafka records.
     *
     * <p>This benchmark measures the combined execution time of:
     *
     * <ul>
     *   <li>Mapping a raw Kafka record using the configured record mapper
     *   <li>Extracting the fields map from the mapped record
     * </ul>
     *
     * @param plan the benchmark state containing the mapper and test records
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public <V> void mapAndFieldsMap(Plan<V> plan, Blackhole bh) {
        MappedRecord map = plan.mapper.map(plan.record);
        Map<String, String> filtered = map.fieldsMap();
        bh.consume(filtered);
    }

    /**
     * Benchmarks the routing performance of a mapped record.
     *
     * <p>This benchmark measures the time required to route a single mapped record to the
     * appropriate subscribed items based on their subscription criteria. The routing operation
     * determines which subscribed items should receive the data.
     *
     * @param plan the benchmark state containing the mapped record and subscribed items
     * @param bh the JMH blackhole used to consume the benchmark result and prevent optimization
     */
    @Benchmark
    public <V> void route(Plan<V> plan, Blackhole bh) {
        Set<SubscribedItem> routed = plan.mappedRecord.route(plan.subscribedItems);
        bh.consume(routed);
    }
}
