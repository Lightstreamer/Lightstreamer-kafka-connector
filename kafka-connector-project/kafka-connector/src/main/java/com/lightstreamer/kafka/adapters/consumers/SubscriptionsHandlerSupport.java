
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

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka.adapters.commons.LogFactory;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger;
import com.lightstreamer.kafka.common.expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.Item;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class SubscriptionsHandlerSupport {

    static SubscriptionsHandler create(
            Supplier<ConsumerTrigger> consumerTrigger, boolean consumeAtStartup) {
        return create(consumerTrigger.get(), consumeAtStartup);
    }

    private static SubscriptionsHandler create(
            ConsumerTrigger consumerTrigger, boolean consumeAtStartup) {
        if (consumeAtStartup) {
            consumerTrigger.startConsuming(SubscribedItems.nop());
            return new NOPSubscriptionsHandler(consumerTrigger);
        }
        return new DefaultSubscriptionsHandler(consumerTrigger);
    }

    abstract static class AbstractSubscriptionsHandler implements SubscriptionsHandler {

        protected final ConsumerTrigger consumerTrigger;

        protected AbstractSubscriptionsHandler(ConsumerTrigger consumerTrigger) {
            this.consumerTrigger = consumerTrigger;
        }

        @Override
        public final boolean isConsuming() {
            return consumerTrigger.isConsuming();
        }
    }

    static class NOPSubscriptionsHandler extends AbstractSubscriptionsHandler {

        private final CompletableFuture<Void> completed = CompletableFuture.completedFuture(null);

        NOPSubscriptionsHandler(ConsumerTrigger consumerTrigger) {
            super(consumerTrigger);
        }

        @Override
        public CompletableFuture<Void> subscribe(String item, Object itemHandle)
                throws SubscriptionException {
            return completed;
        }

        @Override
        public Item unsubscribe(String topic) throws SubscriptionException {
            return null;
        }
    }

    static class DefaultSubscriptionsHandler extends AbstractSubscriptionsHandler {

        private final ItemTemplates<?, ?> itemTemplates;
        private final Logger log;
        private final ConcurrentHashMap<String, SubscribedItem> subscribedItems =
                new ConcurrentHashMap<>();
        private final AtomicInteger itemsCounter = new AtomicInteger(0);
        private CompletableFuture<Void> consuming;

        DefaultSubscriptionsHandler(ConsumerTrigger consumerTrigger) {
            super(consumerTrigger);
            this.itemTemplates = consumerTrigger.config().itemTemplates();
            this.log = LogFactory.getLogger(consumerTrigger.config().connectionName());
        }

        @Override
        public final CompletableFuture<Void> subscribe(String item, Object itemHandle)
                throws SubscriptionException {
            try {
                SubscribedItem newItem = Items.subscribedFrom(item, itemHandle);
                if (!itemTemplates.matches(newItem)) {
                    log.atWarn().log("Item [{}] does not match any defined item templates", item);
                    throw new SubscriptionException(
                            "Item does not match any defined item templates");
                }

                log.atInfo().log("Subscribed to item [{}]", item);
                subscribedItems.put(item, newItem);
                if (itemsCounter.incrementAndGet() == 1) {
                    consuming =
                            consumerTrigger.startConsuming(
                                    SubscribedItems.of(subscribedItems.values()));
                }

                if (consuming.isCompletedExceptionally()) {
                    itemsCounter.set(0);
                }
                return consuming;
            } catch (ExpressionException e) {
                log.atError().setCause(e).log();
                throw new SubscriptionException(e.getMessage());
            }
        }

        public final Item unsubscribe(String item) throws SubscriptionException {
            Item removedItem = subscribedItems.remove(item);
            if (removedItem == null) {
                throw new SubscriptionException(
                        "Unsubscribing from unexpected item [%s]".formatted(item));
            }

            if (itemsCounter.decrementAndGet() == 0) {
                consumerTrigger.stopConsuming();
            }
            return removedItem;
        }

        // Only for testing purposes
        int getItemsCounter() {
            return itemsCounter.get();
        }

        // Only for testing purposes
        Item getSubscribedItem(String itemName) {
            return subscribedItems.get(itemName);
        }
    }
}
