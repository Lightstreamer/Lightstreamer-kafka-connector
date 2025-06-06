
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public interface RecordConsumer<K, V> {

    enum OrderStrategy {
        ORDER_BY_KEY(record -> Objects.toString(record.key(), null)),
        ORDER_BY_PARTITION(record -> record.topic() + "-" + record.partition()),
        UNORDERED(record -> null);

        private final Function<ConsumerRecord<?, ?>, String> sequence;

        OrderStrategy(Function<ConsumerRecord<?, ?>, String> sequence) {
            this.sequence = sequence;
        }

        String getSequence(ConsumerRecord<?, ?> record) {
            return sequence.apply(record);
        }

        public static OrderStrategy from(RecordConsumeWithOrderStrategy strategy) {
            return switch (strategy) {
                case ORDER_BY_KEY -> ORDER_BY_KEY;
                case ORDER_BY_PARTITION -> ORDER_BY_PARTITION;
                case UNORDERED -> UNORDERED;
            };
        }
    }

    interface RecordProcessor<K, V> {

        void process(ConsumerRecord<K, V> record) throws ValueException;

        void useLogger(Logger logger);

        default boolean isCommandEnforceEnabled() {
            return false;
        }
    }

    public interface StartBuildingProcessor<K, V> {

        WithSubscribedItems<K, V> subscribedItems(SubscribedItems subscribedItems);
    }

    interface WithSubscribedItems<K, V> {

        WithEnforceCommandMode<K, V> enforceCommandMode(boolean enforceCommandMode);
    }

    interface WithEnforceCommandMode<K, V> {

        StartBuildingConsumer<K, V> eventListener(ItemEventListener listener);
    }

    public interface StartBuildingConsumer<K, V> {

        WithOffsetService<K, V> offsetService(OffsetService offsetService);
    }

    interface WithOffsetService<K, V> {

        WithLogger<K, V> errorStrategy(RecordErrorHandlingStrategy strategy);
    }

    interface WithLogger<K, V> {

        WithOptionals<K, V> logger(Logger logger);
    }

    interface WithOptionals<K, V> {

        WithOptionals<K, V> threads(int threads);

        WithOptionals<K, V> ordering(OrderStrategy orderStrategy);

        WithOptionals<K, V> preferSingleThread(boolean singleThread);

        RecordConsumer<K, V> build();
    }

    public static <K, V> StartBuildingProcessor<K, V> recordMapper(RecordMapper<K, V> mapper) {
        return RecordConsumerSupport.startBuildingProcessor(mapper);
    }

    public static <K, V> StartBuildingConsumer<K, V> recordProcessor(
            RecordProcessor<K, V> recordProcessor) {
        return RecordConsumerSupport.startBuildingConsumer(recordProcessor);
    }

    static <K, V> ConsumerRecords<K, V> filter(
            ConsumerRecords<K, V> records, Predicate<ConsumerRecord<K, V>> predicate) {
        return new ConsumerRecords<>(
                StreamSupport.stream(records.spliterator(), false)
                        .filter(predicate)
                        .collect(
                                groupingBy(
                                        r -> new TopicPartition(r.topic(), r.partition()),
                                        toList())));
    }

    default ConsumerRecords<K, V> consumeFilteredRecords(
            ConsumerRecords<K, V> records, Predicate<ConsumerRecord<K, V>> predicate) {
        ConsumerRecords<K, V> filtered = filter(records, predicate);
        consumeRecords(filtered);
        return filtered;
    }

    void consumeRecords(ConsumerRecords<K, V> records);

    default int numOfThreads() {
        return 1;
    }

    default Optional<OrderStrategy> orderStrategy() {
        return Optional.empty();
    }

    default boolean isParallel() {
        return numOfThreads() > 1;
    }

    boolean isCommandEnforceEnabled();

    RecordErrorHandlingStrategy errorStrategy();

    default void close() {}
}
