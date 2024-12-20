
/*
 * Copyright (C) 2024 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.OldItemEvent;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper.AdminInterface;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchmarksUtils {

    public static class FakeItemEventListener implements ItemEventListener {

        // Stores all item events sent through the update method.
        public List<Map<String, ?>> events = new ArrayList<>();
        private Blackhole blackHole;
        private AtomicInteger counter;

        public FakeItemEventListener(Blackhole bh) {
            this.blackHole = bh;
            counter = new AtomicInteger();
        }

        @Override
        public void update(
                String itemName,
                com.lightstreamer.interfaces.data.ItemEvent event,
                boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, OldItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(String itemName, Map event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void update(
                String itemName,
                com.lightstreamer.interfaces.data.IndexedItemEvent event,
                boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'update'");
        }

        @Override
        public void smartUpdate(
                Object itemHandle,
                com.lightstreamer.interfaces.data.ItemEvent event,
                boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void smartUpdate(Object itemHandle, OldItemEvent event, boolean isSnapshot) {
            throw new UnsupportedOperationException("Unimplemented method 'smartUpdate'");
        }

        @Override
        public void smartUpdate(Object itemHandle, Map event, boolean isSnapshot) {
            blackHole.consume(event);
            // counter.incrementAndGet();
        }

        @Override
        public void smartUpdate(
                Object itemHandle,
                com.lightstreamer.interfaces.data.IndexedItemEvent event,
                boolean isSnapshot) {}

        @Override
        public void endOfSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'endOfSnapshot'");
        }

        @Override
        public void smartEndOfSnapshot(Object itemHandle) {
            throw new UnsupportedOperationException("Unimplemented method 'smartEndOfSnapshot'");
        }

        @Override
        public void clearSnapshot(String itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'clearSnapshot'");
        }

        @Override
        public void smartClearSnapshot(Object itemHandle) {
            throw new UnsupportedOperationException("Unimplemented method 'smartClearSnapshot'");
        }

        @Override
        public void declareFieldDiffOrder(
                String itemName,
                Map<String, com.lightstreamer.interfaces.data.DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException("Unimplemented method 'declareFieldDiffOrder'");
        }

        @Override
        public void smartDeclareFieldDiffOrder(
                Object itemHandle,
                Map<String, com.lightstreamer.interfaces.data.DiffAlgorithm[]> algorithmsMap) {
            throw new UnsupportedOperationException(
                    "Unimplemented method 'smartDeclareFieldDiffOrder'");
        }

        @Override
        public void failure(Throwable e) {
            throw new UnsupportedOperationException("Unimplemented method 'failure'");
        }

        public void show() {}
    }

    public static class FakeOffsetService implements OffsetService {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

        @Override
        public void commitSync() {}

        @Override
        public void commitAsync() {}

        @Override
        public void updateOffsets(ConsumerRecord<?, ?> record) {}

        @Override
        public void onAsyncFailure(Throwable ve) {}

        @Override
        public ValueException getFirstFailure() {
            return null;
        }

        @Override
        public void initStore(boolean flag, OffsetStoreSupplier storeSupplier) {
            throw new UnsupportedOperationException("Unimplemented method 'initStore'");
        }

        @Override
        public boolean notHasPendingOffset(ConsumerRecord<?, ?> record) {
            throw new UnsupportedOperationException("Unimplemented method 'isAlreadyConsumed'");
        }

        @Override
        public Optional<OffsetStore> offsetStore() {
            throw new UnsupportedOperationException("Unimplemented method 'offsetStore'");
        }

        @Override
        public void commitSyncAndIgnoreErrors() {}

        @Override
        public void initStore(
                OffsetStoreSupplier storeSupplier,
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed) {
            throw new UnsupportedOperationException("Unimplemented method 'initStore'");
        }
    }

    public static class Records {

        static record Guy(String name, String surname, String tag, int age, List<Guy> children) {
            public Guy(String name, String surname, String tag, int age) {
                this(name, surname, tag, age, new ArrayList<>());
            }
        }

        public static List<KafkaRecord<String, JsonNode>> kafkaRecords(
                String topic, int partitions, int numOfRecords, int keySize) {
            ConsumerRecords<String, JsonNode> records =
                    consumerRecords(topic, partitions, numOfRecords, keySize);
            List<KafkaRecord<String, JsonNode>> kafkaRecords = new ArrayList<>(records.count());
            records.forEach(record -> kafkaRecords.add(KafkaRecord.from(record)));
            return kafkaRecords;
        }

        public static ConsumerRecords<String, JsonNode> consumerRecords(
                String topic, int partitions, int numOfRecords, int keys) {
            var recordsToPartition =
                    new HashMap<TopicPartition, List<ConsumerRecord<String, JsonNode>>>();
            IntStream.range(0, partitions)
                    .forEach(
                            p -> {
                                TopicPartition tp = new TopicPartition(topic, p);
                                recordsToPartition.put(
                                        tp,
                                        makeRecords(
                                                tp, numOfRecords / partitions, keys / partitions));
                            });

            return new ConsumerRecords<>(recordsToPartition);
        }

        private static List<ConsumerRecord<String, JsonNode>> makeRecords(
                TopicPartition tp, int recordsPerPartition, int keySize) {
            ObjectMapper om = new ObjectMapper();

            Function<Integer, String> keyGenerator =
                    offset -> String.valueOf(tp.partition() * keySize + offset % keySize);

            List<ConsumerRecord<String, JsonNode>> records = new ArrayList<>(recordsPerPartition);
            for (int i = 0; i < recordsPerPartition; i++) {
                // Setup the record attribute
                String name =
                        new SecureRandom()
                                .ints(20, 48, 122)
                                .filter(Character::isLetterOrDigit)
                                .mapToObj(Character::toString)
                                .collect(Collectors.joining());
                String surname =
                        new SecureRandom()
                                .ints(20, 48, 122)
                                .filter(Character::isLetterOrDigit)
                                .mapToObj(Character::toString)
                                .collect(Collectors.joining());
                int age = new SecureRandom().nextInt(20, 40);
                int sonAge = new SecureRandom().nextInt(0, 4);

                String key = keyGenerator.apply(i);

                // Prepare the Guy object: a parent with two kids
                Guy son1 = new Guy(name + "-son", surname, key + "-son", sonAge);
                Guy son2 = new Guy(name + "-son2", surname, key + "-son2", sonAge + 2);
                Guy parent = new Guy(name, surname, key, age);
                parent.children.add(son1);
                parent.children.add(son2);

                // Build the ConsumerRecord from the JSON and add it to the collection
                JsonNode node = om.valueToTree(parent);
                records.add(new ConsumerRecord<>(tp.topic(), tp.partition(), i, key, node));
            }
            return records;
        }
    }

    private static List<KafkaRecord<String, JsonNode>> initializeRecords(int size)
            throws JsonProcessingException, JsonMappingException {
        ObjectMapper om = new ObjectMapper();

        List<KafkaRecord<String, JsonNode>> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String name =
                    new SecureRandom()
                            .ints(20, 48, 122)
                            .filter(Character::isLetterOrDigit)
                            .mapToObj(Character::toString)
                            .collect(Collectors.joining());
            String surname =
                    new SecureRandom()
                            .ints(20, 48, 122)
                            .filter(Character::isLetterOrDigit)
                            .mapToObj(Character::toString)
                            .collect(Collectors.joining());
            int age = new SecureRandom().nextInt(10, 40);
            String obj =
                    """
            {
                "name": "%s",
                "surname": "%s",
                "age": %d
            }
            """
                            .formatted(name, surname, age);
            JsonNode node = om.readTree(obj);
            records.add(record("MyTopic", "key", node));
        }
        return records;
    }

    private static <K, V> KafkaRecord<K, V> record(String topic, K key, V value) {
        return KafkaRecord.from(
                new ConsumerRecord<>(
                        topic,
                        150,
                        120,
                        ConsumerRecord.NO_TIMESTAMP,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        ConsumerRecord.NULL_SIZE,
                        ConsumerRecord.NULL_SIZE,
                        key,
                        value,
                        new RecordHeaders(),
                        Optional.empty()));
    }

    public static DataExtractor<String, JsonNode> jsonExtractor(
            String schemaName, Map<String, ExtractionExpression> expressions)
            throws ExtractionException {

        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers();
        KeyValueSelectorSuppliers<String, JsonNode> ss =
                new WrapperKeyValueSelectorSuppliers<>(
                        OthersSelectorSuppliers.StringKey(), j.makeValueSelectorSupplier());
        return DataExtractor.extractor(ss, schemaName, expressions);
    }

    public static class FakeAdminInterface implements AdminInterface {

        private final String topic;

        public FakeAdminInterface(String topic) {
            this.topic = topic;
        }

        @Override
        public void close() throws Exception {}

        @Override
        public Set<String> listTopics(ListTopicsOptions options) throws Exception {
            return Collections.singleton(topic);
        }
    }

    @SuppressWarnings("unchecked")
    public static ConsumerTriggerConfig<String, JsonNode> newConfigurator(String topic) {
        File adapterdir;
        try {
            adapterdir = Files.createTempDirectory("adapter_dir").toFile();
            return (ConsumerTriggerConfig<String, JsonNode>)
                    new ConnectorConfigurator(basicParameters(topic, 1, "ORDER_BY_KEY"), adapterdir)
                            .configure();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static ConsumerTriggerConfig<String, JsonNode> newConfigurator(
            String topic, int threads, String ordering) {
        File adapterdir;
        try {
            adapterdir = Files.createTempDirectory("adapter_dir").toFile();
            return (ConsumerTriggerConfig<String, JsonNode>)
                    new ConnectorConfigurator(basicParameters(topic, threads, ordering), adapterdir)
                            .configure();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Map<String, String> basicParameters(String topic, int threads, String ordering) {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
        adapterParams.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "STRING");
        adapterParams.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "JSON");
        adapterParams.put(ConnectorConfig.RECORD_CONSUME_WITH_NUM_THREADS, String.valueOf(threads));
        adapterParams.put(ConnectorConfig.RECORD_CONSUME_WITH_ORDER_STRATEGY, ordering);
        adapterParams.put(
                "item-template.users",
                "users-#{key=KEY,tag=VALUE.tag,sonTag=VALUE.children[0].tag}");
        adapterParams.put("map." + topic + ".to", "item-template.users");
        adapterParams.put("field.name", "#{VALUE.name}");
        adapterParams.put("field.surname", "#{VALUE.surname}");
        adapterParams.put("field.age", "#{VALUE.age}");
        adapterParams.put("field.tag", "#{VALUE.tag}");
        return adapterParams;
    }
}
