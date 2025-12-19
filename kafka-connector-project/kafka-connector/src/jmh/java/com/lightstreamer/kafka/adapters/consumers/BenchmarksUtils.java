
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
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.OldItemEvent;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.consumers.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper.AdminInterface;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
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
import java.util.function.Function;
import java.util.stream.Collectors;

public class BenchmarksUtils {

    public static class FakeItemEventListener implements ItemEventListener {

        // Stores all item events sent through the update method.
        public List<Map<String, ?>> events = new ArrayList<>();
        private Blackhole blackHole;

        public FakeItemEventListener(Blackhole bh) {
            this.blackHole = bh;
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
        public void maybeCommit() {}

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
        public void initStore(
                OffsetStoreSupplier storeSupplier,
                Map<TopicPartition, Long> startOffsets,
                Map<TopicPartition, OffsetAndMetadata> committed) {
            throw new UnsupportedOperationException("Unimplemented method 'initStore'");
        }

        @Override
        public boolean canManageHoles() {
            return false;
        }

        @Override
        public void onConsumerShutdown() {}
    }

    public static class JsonRecords {

        static record Guy(String name, String surname, String tag, int age, List<Guy> children) {
            public Guy(String name, String surname, String tag, int age) {
                this(name, surname, tag, age, new ArrayList<>());
            }
        }

        public static List<KafkaRecord<String, JsonNode>> kafkaRecords(
                String[] topics, int partitions, int numOfRecords, int keySize) {
            ConsumerRecords<String, JsonNode> records =
                    consumerRecords(topics, partitions, numOfRecords, keySize);
            List<KafkaRecord<String, JsonNode>> kafkaRecords = new ArrayList<>(records.count());
            records.forEach(record -> kafkaRecords.add(KafkaRecord.from(record)));
            return kafkaRecords;
        }

        public static ConsumerRecords<String, JsonNode> consumerRecords(
                String[] topics, int partitions, int numOfRecords, int keys) {
            var recordsToPartition =
                    new HashMap<TopicPartition, List<ConsumerRecord<String, JsonNode>>>();
            for (String topic : topics) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    recordsToPartition.put(
                            tp, makeRecords(tp, numOfRecords / partitions, keys / partitions));
                }
            }

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

    public static class ProtoRecords {

        public static List<KafkaRecord<String, DynamicMessage>> kafkaRecords(
                String[] topics, int partitions, int numOfRecords, int keySize) {
            ConsumerRecords<String, DynamicMessage> records =
                    consumerRecords(topics, partitions, numOfRecords, keySize);
            List<KafkaRecord<String, DynamicMessage>> kafkaRecords =
                    new ArrayList<>(records.count());
            records.forEach(record -> kafkaRecords.add(KafkaRecord.from(record)));
            return kafkaRecords;
        }

        public static ConsumerRecords<String, DynamicMessage> consumerRecords(
                String[] topics, int partitions, int numOfRecords, int keys) {
            var recordsToPartition =
                    new HashMap<TopicPartition, List<ConsumerRecord<String, DynamicMessage>>>();
            for (String topic : topics) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    recordsToPartition.put(
                            tp, makeRecords(tp, numOfRecords / partitions, keys / partitions));
                }
            }

            return new ConsumerRecords<>(recordsToPartition);
        }

        private static List<ConsumerRecord<String, DynamicMessage>> makeRecords(
                TopicPartition tp, int recordsPerPartition, int keySize) {
            Function<Integer, String> keyGenerator =
                    offset -> String.valueOf(tp.partition() * keySize + offset % keySize);

            List<ConsumerRecord<String, DynamicMessage>> records =
                    new ArrayList<>(recordsPerPartition);
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
                DynamicMessage parent =
                        DynamicMessage.newBuilder(
                                        com.lightstreamer.kafka.benchmarks.Guy.newBuilder()
                                                .setName(name)
                                                .setSurname(surname)
                                                .setTag(key)
                                                .setAge(age)
                                                .addChildren(
                                                        com.lightstreamer.kafka.benchmarks.Guy
                                                                .newBuilder()
                                                                .setName(name + "-son")
                                                                .setSurname(surname)
                                                                .setTag(key + "-son")
                                                                .setAge(sonAge)
                                                                .build())
                                                .addChildren(
                                                        com.lightstreamer.kafka.benchmarks.Guy
                                                                .newBuilder()
                                                                .setName(name + "-son2")
                                                                .setSurname(surname)
                                                                .setTag(key + "-son2")
                                                                .setAge(sonAge + 2)
                                                                .build())
                                                .build())
                                .build();

                // Build the ConsumerRecord from the JSON and add it to the collection
                records.add(new ConsumerRecord<>(tp.topic(), tp.partition(), i, key, parent));
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
    public static <T> ConsumerTriggerConfig<String, T> newConfigurator(
            String[] topic, String valueType, int templateParams) {
        File adapterdir;
        try {
            adapterdir = Files.createTempDirectory("adapter_dir").toFile();
            if ("PROTOBUF".equals(valueType)) {
                // Copy the descriptor file to the temp directory
                var source = new File("./build/generated/sources/proto/jmh/descriptor_set.desc");
                var target = new File(adapterdir, "descriptor_set.desc");
                Files.copy(source.toPath(), target.toPath());
            }
            return (ConsumerTriggerConfig<String, T>)
                    new ConnectorConfigurator(
                                    basicParameters(topic, valueType, templateParams), adapterdir)
                            .configure();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static Map<String, String> basicParameters(
            String[] topics, String valueType, int templateParams) {
        List<String> templates =
                List.of(
                        "users-#{key=KEY}",
                        "users-#{key=KEY,tag=VALUE.tag}",
                        "users-#{key=KEY,tag=VALUE.tag,sonTag=VALUE.children[0].tag}");
        System.out.println("Using template: " + templates.get(templateParams - 1));
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
        adapterParams.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "STRING");
        adapterParams.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, valueType);
        if (valueType.equals("PROTOBUF")) {
            adapterParams.put(
                    ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH, "descriptor_set.desc");
            adapterParams.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE, "Guy");
        }
        adapterParams.put("item-template.users", templates.get(templateParams - 1));
        for (String t : topics) {
            adapterParams.put("map." + t + ".to", "item-template.users");
        }
        adapterParams.put("field.name", "#{VALUE.name}");
        adapterParams.put("field.surname", "#{VALUE.surname}");
        adapterParams.put("field.age", "#{VALUE.age}");
        adapterParams.put("field.tag", "#{VALUE.tag}");
        return adapterParams;
    }

    public static <T> RecordMapper<String, T> newRecordMapper(
            ConsumerTriggerConfig<String, T> config) {
        return RecordMapper.<String, T>builder()
                .withCanonicalItemExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    public static SubscribedItems subscriptions(int subscriptions) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        for (int i = 0; i < subscriptions; i++) {
            String key = String.valueOf(i);
            String input = "users-[key=%s,tag=%s,sonTag=%s]".formatted(key, key, key + "-son");
            subscribedItems.add(Items.subscribedFrom(input, new Object()));
        }
        return subscribedItems;
    }
}
