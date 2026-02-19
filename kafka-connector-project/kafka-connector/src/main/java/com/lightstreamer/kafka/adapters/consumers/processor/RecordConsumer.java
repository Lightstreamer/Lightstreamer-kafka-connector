
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.Offsets.OffsetService;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.RecordBatch;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public interface RecordConsumer<K, V> {

    enum OrderStrategy {
        ORDER_BY_KEY(record -> Objects.toString(record.key(), null)),
        ORDER_BY_PARTITION(record -> record.topic() + "-" + record.partition()),
        UNORDERED(record -> null);

        private final Function<KafkaRecord<?, ?>, String> sequence;

        OrderStrategy(Function<KafkaRecord<?, ?>, String> sequence) {
            this.sequence = sequence;
        }

        String getSequence(KafkaRecord<?, ?> record) {
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

        enum ProcessUpdatesType {
            DEFAULT,
            COMMAND,
            AUTO_COMMAND_MODE;

            boolean allowConcurrentProcessing() {
                return this != COMMAND;
            }
        }

        void process(KafkaRecord<K, V> record) throws ValueException;

        void processAsSnapshot(KafkaRecord<K, V> record, SubscribedItem subscribedItem)
                throws ValueException;

        void useLogger(Logger logger);

        ProcessUpdatesType processUpdatesType();
    }

    public interface StartBuildingProcessor<K, V> {

        WithSubscribedItems<K, V> subscribedItems(SubscribedItems subscribedItems);
    }

    interface WithSubscribedItems<K, V> {

        WithEnforceCommandMode<K, V> commandMode(CommandModeStrategy commandModeStrategy);
    }

    interface WithEnforceCommandMode<K, V> {

        StartBuildingConsumer<K, V> eventListener(EventListener listener);
    }

    public interface StartBuildingConsumer<K, V> {

        WithOffsetService<K, V> offsetService(OffsetService offsetService);
    }

    interface WithOffsetService<K, V> {

        WithLogger<K, V> errorStrategy(RecordErrorHandlingStrategy errorHandlingStrategy);
    }

    interface WithLogger<K, V> {

        WithOptionals<K, V> logger(Logger logger);
    }

    interface WithOptionals<K, V> {

        WithOptionals<K, V> threads(int threads);

        WithOptionals<K, V> ordering(OrderStrategy orderStrategy);

        WithOptionals<K, V> preferSingleThread(boolean singleThread);

        WithOptionals<K, V> monitor(Monitor monitor);

        RecordConsumer<K, V> build();
    }

    public static <K, V> StartBuildingProcessor<K, V> recordMapper(RecordMapper<K, V> mapper) {
        return RecordConsumerSupport.startBuildingProcessor(mapper);
    }

    public static <K, V> StartBuildingConsumer<K, V> recordProcessor(
            RecordProcessor<K, V> recordProcessor) {
        return RecordConsumerSupport.startBuildingConsumer(recordProcessor);
    }

    /**
     * Consumes a batch of records.
     *
     * <p>This method processes a batch of records of the specified generic types K and V. The
     * implementation is responsible for handling the records within the batch according to the
     * business logic requirements.
     *
     * @param <K> the type of the keys in the record batch
     * @param <V> the type of the values in the record batch
     * @param batch the batch of records to be consumed, must not be null
     */
    void consumeBatch(RecordBatch<K, V> batch);

    boolean hasFailedAsynchronously();

    default int numOfThreads() {
        return 1;
    }

    default Optional<OrderStrategy> orderStrategy() {
        return Optional.empty();
    }

    default boolean isParallel() {
        return numOfThreads() > 1;
    }

    RecordErrorHandlingStrategy errorStrategy();

    RecordProcessor<K, V> recordProcessor();

    Monitor monitor();

    default void close() {}

    // Only for testing purposes
    boolean isClosed();
}
