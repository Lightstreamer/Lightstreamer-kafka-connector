
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.common.mapping.Items.subscribedFrom;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.junit.jupiter.api.Test;

import java.util.Optional;

public class SubscribedItemsTest {

    @Test
    public void shouldManageSubscriptions() {
        SubscribedItems subscribedItems = SubscribedItems.create();
        assertThat(subscribedItems.acceptSubscriptions()).isTrue();
        assertThat(subscribedItems).isEmpty();

        SubscribedItem item1 = subscribedFrom("anItem");
        subscribedItems.addItem("anItem", item1);
        assertThat(subscribedItems).hasSize(1);
        assertThat(subscribedItems.getItem("anItem")).hasValue(item1);

        SubscribedItem item2 = subscribedFrom("anItem2");
        subscribedItems.addItem("anItem2", item2);
        assertThat(subscribedItems).hasSize(2);
        assertThat(subscribedItems.getItem("anItem2")).hasValue(item2);

        Optional<SubscribedItem> removedItem1 = subscribedItems.removeItem("anItem");
        assertThat(removedItem1).hasValue(item1);
        assertThat(subscribedItems.getItem("anItem")).isEmpty();
        assertThat(subscribedItems).hasSize(1);

        Optional<SubscribedItem> removedItem2 = subscribedItems.removeItem("anItem2");
        assertThat(removedItem2).hasValue(item2);
        assertThat(subscribedItems.getItem("anItem2")).isEmpty();
        assertThat(subscribedItems).isEmpty();
    }

    @Test
    public void shouldNotManageSubscriptionsInNop() {
        SubscribedItems subscribedItems = SubscribedItems.nop();
        assertThat(subscribedItems.acceptSubscriptions()).isFalse();

        SubscribedItem item = subscribedFrom("anItem");
        subscribedItems.addItem("anItem", item);
        assertThat(subscribedItems).isEmpty();

        Optional<SubscribedItem> removedItem = subscribedItems.removeItem("anItem");
        assertThat(removedItem).isEmpty();
    }
}
