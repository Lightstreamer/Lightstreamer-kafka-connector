
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BenchmarksUtils {

    private static List<String> TEMPLATES =
            List.of(
                    "users-#{key=KEY}",
                    "users-#{key=KEY,tag=VALUE.tag}",
                    "users-#{key=KEY,tag=VALUE.tag,sonTag=VALUE.children[0].tag}");

    private static List<String> SUBSCRIPTIONS =
            List.of("users-[key=%s]", "users-[key=%s,tag=%s]", "users-[key=%s,tag=%s,sonTag=%s]");

    public static class FakeEventListener implements EventListener {

        private Blackhole blackHole;
        private AtomicInteger counter;

        public FakeEventListener(Blackhole bh) {
            this.blackHole = bh;
            this.counter = new AtomicInteger();
        }

        @Override
        public void update(SubscribedItem item, Map<String, String> updates, boolean isSnapshot) {
            blackHole.consume(updates);
            // System.out.println(Received update for item " + item.asCanonicalItemName() + ": " +
            // updates);
            counter.incrementAndGet();
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

        public void show() {
            System.out.println("Total events processed: " + counter.get());
            counter.set(0);
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

    public static class RawRecord {

        private final String topic;
        private final byte[] rawKey;
        private final byte[] rawBytes;
        private final int partition;
        private final int offset;

        public RawRecord(String topic, int partition, int offset, byte[] rawKey, byte[] rawValue) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.rawKey = rawKey;
            this.rawBytes = rawValue;
        }

        public String topic() {
            return topic;
        }

        public int partition() {
            return partition;
        }

        public int offset() {
            return offset;
        }

        public byte[] rawKey() {
            return rawKey;
        }

        public byte[] rawValue() {
            return rawBytes;
        }
    }

    public static ConsumerRecords<byte[], byte[]> pollRecordsFromRaw(
            String[] topics, int partitions, List<RawRecord> rawRecords) {
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        for (int i = 0; i < partitions; i++) {
            for (int t = 0; t < topics.length; t++) {
                TopicPartition tp = new TopicPartition(topics[t], i);
                recordsMap.put(tp, new java.util.ArrayList<>());
            }
        }

        for (int i = 0; i < rawRecords.size(); i++) {
            RawRecord rawRecord = rawRecords.get(i);
            ConsumerRecord<byte[], byte[]> consumerRecord =
                    new ConsumerRecord<>(
                            rawRecord.topic(),
                            rawRecord.partition(),
                            rawRecord.offset(),
                            rawRecord.rawKey(),
                            rawRecord.rawValue());
            TopicPartition tp = new TopicPartition(rawRecord.topic(), rawRecord.partition());
            recordsMap.get(tp).add(consumerRecord);
        }

        return new ConsumerRecords<>(recordsMap);
    }

    private static String generateKey(TopicPartition tp, int keySize, int index) {
        return String.valueOf(tp.partition() * keySize + index % keySize);
    }

    public static class JsonRecords {

        static record Guy(String name, String surname, String tag, int age, List<Guy> children) {
            public Guy(String name, String surname, String tag, int age) {
                this(name, surname, tag, age, new ArrayList<>());
            }
        }

        public static List<RawRecord> rawRecords(
                String[] topics, int partitions, int numOfRecords, int keySize) {
            List<RawRecord> allRecords = new ArrayList<>(numOfRecords);
            ObjectMapper om = new ObjectMapper();
            for (String topic : topics) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    List<JsonNode> messages =
                            generateMessages(tp, numOfRecords / partitions, keySize / partitions);
                    for (int i = 0; i < messages.size(); i++) {
                        byte[] rawKey = generateKey(tp, keySize / partitions, i).getBytes();
                        try {
                            allRecords.add(
                                    new RawRecord(
                                            tp.topic(),
                                            tp.partition(),
                                            i,
                                            rawKey,
                                            om.writeValueAsBytes(messages.get(i))));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            return allRecords;
        }

        public static ConsumerRecords<byte[], byte[]> pollRecords(
                String[] topics, int partitions, int numRecords, int keySize) {

            List<RawRecord> rawRecords = rawRecords(topics, partitions, numRecords, keySize);
            return pollRecordsFromRaw(topics, partitions, rawRecords);
        }

        private static List<JsonNode> generateMessages(
                TopicPartition tp, int recordsPerPartition, int keySize) {
            ObjectMapper om = new ObjectMapper();

            Function<Integer, String> keyGenerator =
                    offset -> String.valueOf(tp.partition() * keySize + offset % keySize);

            List<JsonNode> messages = new ArrayList<>(recordsPerPartition);
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
                messages.add(node);
            }
            return messages;
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
                        byte[] rawKey = generateKey(tp, keySize / partitions, i).getBytes();
                        allRecords.add(
                                new RawRecord(
                                        tp.topic(),
                                        tp.partition(),
                                        i,
                                        rawKey,
                                        messages.get(i).toByteArray()));
                    }
                }
            }
            return allRecords;
        }

        public static ConsumerRecords<byte[], byte[]> pollRecords(
                String[] topics, int partitions, int numRecords, int keySize) {

            List<RawRecord> rawRecords = rawRecords(topics, partitions, numRecords, keySize);
            return pollRecordsFromRaw(topics, partitions, rawRecords);
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
                // var source =
                //         new File(

                // "./kafka-connector-project/kafka-connector/build/generated/sources/proto/jmh/descriptor_set.desc");
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
        String template = TEMPLATES.get(templateParams - 1);
        System.out.println("Using template: " + template);
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
        adapterParams.put("item-template.users", template);
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
                .withCanonicalItemExtractors(config.itemTemplates().groupExtractors())
                .withFieldExtractor(config.fieldsExtractor())
                .build();
    }

    public static SubscribedItems subscriptions(
            int subscriptions, EventListener listener, int numOfTemplateParams) {
        SubscribedItems subscribedItems = SubscribedItems.create();
        for (int i = 0; i < subscriptions; i++) {
            String key = String.valueOf(i);

            Object[] params =
                    switch (numOfTemplateParams) {
                        case 1 -> new Object[] {key};
                        case 2 -> new Object[] {key, key};
                        case 3 -> new Object[] {key, key, key + "-son"};
                        default ->
                                throw new IllegalArgumentException(
                                        "Invalid subscription number: " + numOfTemplateParams);
                    };
            String input = SUBSCRIPTIONS.get(numOfTemplateParams - 1).formatted(params);
            SubscribedItem item = Items.subscribedFrom(input, new Object());
            item.enableRealtimeEvents(listener);
            subscribedItems.addItem(item);
        }
        return subscribedItems;
    }
}
