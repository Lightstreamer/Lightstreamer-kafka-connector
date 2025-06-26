
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

package com.lightstreamer.kafka.adapters.consumers.wrapper;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers.KeyValueDeserializers;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface KafkaConsumerWrapper {

    record Config<K, V>(
            String connectionName,
            Properties consumerProperties,
            ItemTemplates<K, V> itemTemplates,
            DataExtractor<K, V> fieldsExtractor,
            KeyValueDeserializers<K, V> deserializers,
            RecordErrorHandlingStrategy errorHandlingStrategy,
            CommandModeStrategy commandModeStrategy,
            Concurrency concurrency) {}

    record Concurrency(RecordConsumeWithOrderStrategy orderStrategy, int threads) {

        public boolean isParallel() {
            return threads() != 1;
        }
    }

    CompletableFuture<Void> startConsuming(SubscribedItems items);

    boolean isConsuming();

    void stopConsuming();

    Config<?, ?> config();

    public static <K, V> KafkaConsumerWrapper create(
            Config<K, V> config,
            MetadataListener metadataListener,
            ItemEventListener eventListener,
            Supplier<Consumer<K, V>> consumer) {
        return KafkaConsumerWrapperSupport.create(
                config, metadataListener, eventListener, consumer);
    }

    public static <K, V> Supplier<Consumer<K, V>> defaultConsumerSupplier(Config<K, V> config) {
        return () ->
                new KafkaConsumer<>(
                        config.consumerProperties(),
                        config.deserializers().keyDeserializer(),
                        config.deserializers().valueDeserializer());
    }
}
