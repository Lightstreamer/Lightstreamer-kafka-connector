
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.ConnectorConfigurator;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.consumers.deserialization.Deferred;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetStore;
import com.lightstreamer.kafka.adapters.consumers.wrapper.KafkaConsumerWrapperConfig.Config;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BenchmarksUtils {

    public static class FakeItemEventListener implements EventListener {

        // Stores all item events sent through the update method.
        public List<Map<String, ?>> events = new ArrayList<>();
        private Blackhole blackHole;

        public FakeItemEventListener(Blackhole bh) {
            this.blackHole = bh;
        }

        @Override
        public void update(SubscribedItem item, Map<String, String> updates, boolean isSnapshot) {
            blackHole.consume(updates);
            // counter.incrementAndGet();
        }

        @Override
        public void endOfSnapshot(SubscribedItem itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'endOfSnapshot'");
        }

        @Override
        public void clearSnapshot(SubscribedItem itemName) {
            throw new UnsupportedOperationException("Unimplemented method 'clearSnapshot'");
        }

        @Override
        public void failure(Exception e) {
            throw new UnsupportedOperationException("Unimplemented method 'failure'");
        }
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
        public void updateOffsets(KafkaRecord<?, ?> record) {}

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
        public boolean notHasPendingOffset(KafkaRecord<?, ?> record) {
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

        @Override
        public boolean canManageHoles() {
            return false;
        }
    }

    public static class FakeMetadataListener implements MetadataListener {

        @Override
        public void forceUnsubscription(String item) {}

        @Override
        public void forceUnsubscriptionAll() {}
    }

    public static class JsonRecords {

        static record Guy(String name, String surname, String tag, int age, List<Guy> children) {
            public Guy(String name, String surname, String tag, int age) {
                this(name, surname, tag, age, new ArrayList<>());
            }
        }

        public static List<KafkaRecord<String, JsonNode>> kafkaRecords(
                String[] topics, int partitions, int numOfRecords, int keySize) {
            ConsumerRecords<Deferred<String>, Deferred<JsonNode>> records =
                    consumerRecords(topics, partitions, numOfRecords, keySize);
            List<KafkaRecord<String, JsonNode>> kafkaRecords = new ArrayList<>(records.count());
            records.forEach(record -> kafkaRecords.add(KafkaRecord.from(record)));
            return kafkaRecords;
        }

        public static ConsumerRecords<Deferred<String>, Deferred<JsonNode>> consumerRecords(
                String[] topics, int partitions, int numOfRecords, int keys) {
            var recordsToPartition =
                    new HashMap<
                            TopicPartition,
                            List<ConsumerRecord<Deferred<String>, Deferred<JsonNode>>>>();
            for (String topic : topics) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    recordsToPartition.put(
                            tp, makeRecords(tp, numOfRecords / partitions, keys / partitions));
                }
            }

            return new ConsumerRecords<>(recordsToPartition);
        }

        private static List<ConsumerRecord<Deferred<String>, Deferred<JsonNode>>> makeRecords(
                TopicPartition tp, int recordsPerPartition, int keySize) {
            ObjectMapper om = new ObjectMapper();

            Function<Integer, String> keyGenerator =
                    offset -> String.valueOf(tp.partition() * keySize + offset % keySize);

            List<ConsumerRecord<Deferred<String>, Deferred<JsonNode>>> records =
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
                Guy son1 = new Guy(name + "-son", surname, key + "-son", sonAge);
                Guy son2 = new Guy(name + "-son2", surname, key + "-son2", sonAge + 2);
                Guy parent = new Guy(name, surname, key, age);
                parent.children.add(son1);
                parent.children.add(son2);

                // Build the ConsumerRecord from the JSON and add it to the collection
                JsonNode node = om.valueToTree(parent);
                records.add(
                        new ConsumerRecord<>(
                                tp.topic(),
                                tp.partition(),
                                i,
                                Deferred.resolved(key),
                                Deferred.resolved(node)));
            }
            return records;
        }
    }

    public static class RawRecord {

        private final String topic;
        private final String key;
        private final byte[] rawBytes;
        private final int partition;
        private final int offset;

        public RawRecord(String topic, int partition, int offset, String key, byte[] rawBytes) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.rawBytes = rawBytes;
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public int getOffset() {
            return offset;
        }

        public String getKey() {
            return key;
        }

        public byte[] getRawBytes() {
            return rawBytes;
        }
    }

    public static class ProtoRecords {

        public static List<RawRecord> rawRecords(
                String[] topics, int partitions, int numOfRecords, int keySize) {
            List<RawRecord> allRecords = new ArrayList<>(numOfRecords);
            for (String topic : topics) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    List<DynamicMessage> messages =
                            generateMessages(tp, numOfRecords / partitions, keySize / partitions);
                    for (int i = 0; i < messages.size(); i++) {
                        String key = generateKey(tp, keySize / partitions, i);
                        allRecords.add(
                                new RawRecord(
                                        tp.topic(),
                                        tp.partition(),
                                        i,
                                        key,
                                        messages.get(i).toByteArray()));
                    }
                }
            }
            return allRecords;
        }

        public static List<KafkaRecord<String, DynamicMessage>> kafkaRecords(
                String[] topics, int partitions, int numOfRecords, int keySize) {
            List<KafkaRecord<String, DynamicMessage>> allRecords = new ArrayList<>(numOfRecords);
            for (String topic : topics) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    List<DynamicMessage> messages =
                            generateMessages(tp, numOfRecords / partitions, keySize / partitions);
                    for (int i = 0; i < messages.size(); i++) {
                        String key = generateKey(tp, keySize / partitions, i);
                        allRecords.add(
                                KafkaRecord.from(
                                        new ConsumerRecord<>(
                                                tp.topic(),
                                                tp.partition(),
                                                i,
                                                Deferred.resolved(key),
                                                Deferred.resolved(messages.get(i)))));
                    }
                }
            }
            return allRecords;
        }

        public static ConsumerRecords<Deferred<String>, Deferred<DynamicMessage>> consumerRecords(
                String[] topics, int partitions, int numOfRecords, int keySize) {
            var recordsToPartition =
                    new HashMap<
                            TopicPartition,
                            List<ConsumerRecord<Deferred<String>, Deferred<DynamicMessage>>>>();
            for (String topic : topics) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    List<DynamicMessage> messages =
                            generateMessages(tp, numOfRecords / partitions, keySize / partitions);
                    List<ConsumerRecord<Deferred<String>, Deferred<DynamicMessage>>> records =
                            new ArrayList<>();

                    for (int i = 0; i < messages.size(); i++) {
                        String key = generateKey(tp, keySize / partitions, i);
                        records.add(
                                new ConsumerRecord<>(
                                        tp.topic(),
                                        tp.partition(),
                                        i,
                                        Deferred.resolved(key),
                                        Deferred.resolved(messages.get(i))));
                    }
                    recordsToPartition.put(tp, records);
                }
            }

            return new ConsumerRecords<>(recordsToPartition);
        }

        public static ConsumerRecords<Deferred<String>, Deferred<DynamicMessage>>
                resolvedConsumerRecords(
                        List<RawRecord> rawRecords, Deserializer<DynamicMessage> deserializer) {
            var recordsToPartition =
                    new HashMap<
                            TopicPartition,
                            List<ConsumerRecord<Deferred<String>, Deferred<DynamicMessage>>>>();
            for (RawRecord rawRecord : rawRecords) {
                TopicPartition tp =
                        new TopicPartition(rawRecord.getTopic(), rawRecord.getPartition());
                recordsToPartition
                        .computeIfAbsent(tp, k -> new ArrayList<>())
                        .add(
                                new ConsumerRecord<>(
                                        rawRecord.getTopic(),
                                        rawRecord.getPartition(),
                                        rawRecord.getOffset(),
                                        Deferred.resolved(rawRecord.getKey()),
                                        Deferred.eager(
                                                deserializer,
                                                rawRecord.getTopic(),
                                                rawRecord.getRawBytes())));
            }

            return new ConsumerRecords<>(recordsToPartition);
        }

        public static ConsumerRecords<Deferred<String>, Deferred<DynamicMessage>>
                deferredConsumerRecords(
                        List<RawRecord> rawRecords, Deserializer<DynamicMessage> deserializer) {
            var recordsToPartition =
                    new HashMap<
                            TopicPartition,
                            List<ConsumerRecord<Deferred<String>, Deferred<DynamicMessage>>>>();
            for (RawRecord rawRecord : rawRecords) {
                TopicPartition tp =
                        new TopicPartition(rawRecord.getTopic(), rawRecord.getPartition());
                recordsToPartition
                        .computeIfAbsent(tp, k -> new ArrayList<>())
                        .add(
                                new ConsumerRecord<>(
                                        rawRecord.getTopic(),
                                        rawRecord.getPartition(),
                                        rawRecord.getOffset(),
                                        Deferred.resolved(rawRecord.getKey()),
                                        Deferred.lazy(
                                                deserializer,
                                                rawRecord.getTopic(),
                                                rawRecord.getRawBytes())));
            }

            return new ConsumerRecords<>(recordsToPartition);
        }

        private static String generateKey(TopicPartition tp, int keySize, int index) {
            return String.valueOf(tp.partition() * keySize + index % keySize);
        }

        private static List<DynamicMessage> generateMessages(
                TopicPartition tp, int recordsPerPartition, int keySize) {

            List<DynamicMessage> messages = new ArrayList<>(recordsPerPartition);
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

                String key = generateKey(tp, keySize, i);

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
                messages.add(parent);
            }
            return messages;
        }
    }

    public static ConnectorConfigurator newConfigurator(
            String[] topic, String valueType, int templateParams) {
        File adapterDir;
        try {
            adapterDir = Files.createTempDirectory("adapter_dir").toFile();
            if ("PROTOBUF".equals(valueType)) {
                // Copy the descriptor file to the temp directory
                var source = new File("./build/generated/sources/proto/jmh/descriptor_set.desc");
                var target = new File(adapterDir, "descriptor_set.desc");
                Files.copy(source.toPath(), target.toPath());
            }
            return new ConnectorConfigurator(
                    basicParameters(topic, valueType, templateParams), adapterDir);
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

    public static <T> RecordMapper<String, T> newRecordMapper(Config<String, T> config) {
        return RecordMapper.<String, T>builder()
                .withTemplateExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    public static SubscribedItems subscriptions(int subscriptions) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        for (int i = 0; i < subscriptions; i++) {
            String key = String.valueOf(i);
            String input = "users-[key=%s,tag=%s,sonTag=%s]".formatted(key, key, key + "-son");
            subscribedItems.addItem(Items.subscribedFrom(input, new Object()));
        }
        return subscribedItems;
    }
}
