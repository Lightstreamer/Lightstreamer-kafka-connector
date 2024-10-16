
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

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.OffsetService;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public interface RecordConsumer<K, V> {

    enum OrderStrategy {
        ORDER_BY_KEY(record -> Objects.toString(record.key(), null)),
        ORDER_BY_PARTITION(record -> String.valueOf(record.partition())),
        UNORDERD(record -> null);

        private Function<ConsumerRecord<?, ?>, String> sequence;

        OrderStrategy(Function<ConsumerRecord<?, ?>, String> sequence) {
            this.sequence = sequence;
        }

        String getSequence(ConsumerRecord<?, ?> record) {
            return sequence.apply(record);
        }
    }

    interface RecordProcessor<K, V> {

        void process(ConsumerRecord<K, V> record) throws ValueException;

        void useLogger(Logger logger);
    }

    public interface StartBuildingProcessor<K, V> {

        WithItemTemplates<K, V> itemTemplates(ItemTemplates<K, V> templates);
    }

    interface WithItemTemplates<K, V> {

        WithFieldsExtractor<K, V> fieldsExtractor(DataExtractor<K, V> fieldsExtractor);
    }

    interface WithFieldsExtractor<K, V> {

        WithSubscribedItems<K, V> subscribedItems(Collection<SubscribedItem> subscribedItems);
    }

    interface WithSubscribedItems<K, V> {

        StartBuildingConsumer<K, V> eventListener(ItemEventListener listener);
    }

    public interface StartBuildingConsumer<K, V> {

        WihtOffsetService<K, V> offsetService(OffsetService offsetService);
    }

    interface WihtOffsetService<K, V> {

        WithLogger<K, V> errorStrategy(RecordErrorHandlingStrategy stragey);
    }

    interface WithLogger<K, V> {

        WithOptionals<K, V> logger(Logger logger);
    }

    interface WithOptionals<K, V> {

        WithOptionals<K, V> threads(int threads, OrderStrategy orderStrategy);

        WithOptionals<K, V> threads(int threads);

        WithOptionals<K, V> preferSingleThread(boolean singleThread);

        RecordConsumer<K, V> build();
    }

    public static <K, V> StartBuildingProcessor<K, V> recordMapper(RecordMapper<K, V> mapper) {
        return new RecordConsumerSupport.StartBuildingProcessorBuilderImpl<>(mapper);
    }

    public static <K, V> StartBuildingConsumer<K, V> recordProcessor(
            RecordProcessor<K, V> recordProcessor) {
        return new RecordConsumerSupport.StartBuildingConsumerImpl<>(recordProcessor);
    }

    default void consumeFilteredRecords(
            ConsumerRecords<K, V> records, Predicate<ConsumerRecord<K, V>> predicate) {}

    default void consumeRecords(ConsumerRecords<K, V> records) {
        records.forEach(this::consumeSingleRecord);
    }

    void consumeSingleRecord(ConsumerRecord<K, V> record);

    default void close() {}
}
