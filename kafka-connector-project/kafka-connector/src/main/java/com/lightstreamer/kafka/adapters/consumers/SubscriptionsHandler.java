
/*
 * Copyright (C) 2025 Lightstreamer Srl
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
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.consumers.wrapper.ConsumerWrapper;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import java.util.function.Function;

public interface SubscriptionsHandler {

    /**
     * Creates a new {@code SubscriptionWrapper} instance with the specified configuration, metadata
     * listener, event listener, and startup consumption flag.
     *
     * @param config the consumer trigger configuration
     * @param metadataListener the metadata listener to be used
     * @param eventListener the item event listener to be notified
     * @param consumeAtStartup whether to start consuming messages from Kafka at startup
     * @return a new {@code SubscriptionWrapper} instance
     */
    static SubscriptionsHandler create(
            ConsumerTriggerConfig<?, ?> config,
            MetadataListener metadataListener,
            ItemEventListener eventListener,
            boolean consumeAtStartup) {
        return SubscriptionsHandlerSupport.create(
                () -> ConsumerTrigger.create(config, metadataListener, eventListener),
                consumeAtStartup);
    }

    static <K, V> SubscriptionsHandler create(
            ConsumerTriggerConfig<K, V> config,
            MetadataListener metadataListener,
            Function<SubscribedItems, ConsumerWrapper<K, V>> consumerWrapper,
            boolean consumeAtStartup) {
        return SubscriptionsHandlerSupport.create(
                () -> ConsumerTrigger.create(config, metadataListener, consumerWrapper),
                consumeAtStartup);
    }

    void subscribe(String item, Object itemHandle) throws SubscriptionException;

    Item unsubscribe(String topic) throws SubscriptionException;

    boolean isConsuming();
}
