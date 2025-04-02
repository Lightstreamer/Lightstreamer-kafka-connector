
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

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.MetadataListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers.KeyValueDeserializers;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public interface ConsumerTrigger {

    public interface ConsumerTriggerConfig<K, V> {

        String connectionName();

        Properties consumerProperties();

        ItemTemplates<K, V> itemTemplates();

        DataExtractor<K, V> fieldsExtractor();

        KeyValueDeserializers<K, V> deserializers();

        RecordErrorHandlingStrategy errorHandlingStrategy();

        boolean isCommandEnforceEnabled();

        Concurrency concurrency();

        interface Concurrency {
            RecordConsumeWithOrderStrategy orderStrategy();

            int threads();
        }
    }

    boolean isSnapshotAvailable();

    CompletableFuture<Void> subscribe(String item, Object itemHandle) throws SubscriptionException;

    Item unsubscribe(String topic) throws SubscriptionException;

    static ConsumerTrigger create(
            ConsumerTriggerConfig<?, ?> config,
            MetadataListener metadataListener,
            ItemEventListener eventListener) {
        return new ConsumerTriggerImpl<>(config, metadataListener, eventListener);
    }
}
