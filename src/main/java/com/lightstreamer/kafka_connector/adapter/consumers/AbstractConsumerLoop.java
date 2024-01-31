/*
*
* Copyright (c) Lightstreamer Srl
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapter.ConsumerLoopConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.commons.MetadataListener;
import com.lightstreamer.kafka_connector.adapter.Loop;
import com.lightstreamer.kafka_connector.adapter.mapping.Items;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;

public abstract class AbstractConsumerLoop<K, V> implements Loop {

    protected static Logger log = LoggerFactory.getLogger(AbstractConsumerLoop.class);

    protected final ConsumerLoopConfig<K, V> config;
    protected final ConcurrentHashMap<String, Item> subscribedItems = new ConcurrentHashMap<>();
    protected final AtomicInteger itemsCounter = new AtomicInteger(0);
    protected Object infoItemhande;

    protected final MetadataListener metadataListener;

    protected AbstractConsumerLoop(ConsumerLoopConfig<K, V> config, MetadataListener metadataListener) {
        this.config = config;
        this.metadataListener = metadataListener;
    }

    public final int getItemsCounter() {
        return itemsCounter.get();
    }

    @Override
    public final Item subscribe(String item, Object itemHandle) throws SubscriptionException {
        Item newItem = Items.itemFrom(item, itemHandle);
        if (config.itemTemplates().matches(newItem)) {
            log.atInfo().log("Subscribed to {}", item);
        } else {
            throw new SubscriptionException("Item does not match any defined item templates");
        }

        subscribedItems.put(item, newItem);
        if (itemsCounter.addAndGet(1) == 1) {
            startConsuming();
        }
        return newItem;
    }

    abstract void startConsuming() throws SubscriptionException;

    @Override
    public final Item unsubscribe(String item) throws SubscriptionException {
        Item removedItem = subscribedItems.remove(item);
        if (removedItem == null) {
            throw new SubscriptionException("Unsubscribing unexpected item [%s]".formatted(item));
        }

        if (itemsCounter.decrementAndGet() == 0) {
            stopConsuming();
        }
        return removedItem;
    }

    abstract void stopConsuming();
}
