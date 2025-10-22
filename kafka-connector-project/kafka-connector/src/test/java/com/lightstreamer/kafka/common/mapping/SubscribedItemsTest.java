
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class SubscribedItemsTest {

    private SubscribedItems subscribedItems;

    @BeforeEach
    public void setUp() {
        subscribedItems = SubscribedItems.create();
    }

    @Test
    public void shouldAddAndRetrieveSimpleItems() {
        SubscribedItem testItem1 = Items.subscribedFrom("item1");
        SubscribedItem testItem2 = Items.subscribedFrom("item2");
        subscribedItems.addItem(testItem1);
        subscribedItems.addItem(testItem2);

        assertThat(subscribedItems.getItem("item1")).hasValue(testItem1);
        assertThat(subscribedItems.getItem("item2")).hasValue(testItem2);
    }

    @Test
    public void shouldAddAndRetrieveCanonicalItems() {
        SubscribedItem testItem1 = Items.subscribedFrom("item-[b=2,a=1]");
        subscribedItems.addItem(testItem1);

        // Retrieve the item from its canonical representation
        assertThat(subscribedItems.getItem("item-[a=1,b=2]")).hasValue(testItem1);
    }

    @Test
    public void shouldReplaceItemWhenAddingDuplicate() {
        SubscribedItem testItem1 = Items.subscribedFrom("item1");
        SubscribedItem testItem1Duplicate = Items.subscribedFrom("item1");

        subscribedItems.addItem(testItem1);
        assertThat(subscribedItems.size()).isEqualTo(1);
        subscribedItems.addItem(testItem1Duplicate);
        assertThat(subscribedItems.size()).isEqualTo(1);

        assertThat(subscribedItems.getItem("item1")).hasValue(testItem1Duplicate);
    }

    @Test
    public void shouldReturnNullForNonExistentItem() {
        assertThat(subscribedItems.getItem("nonexistent")).isEmpty();
    }

    @Test
    public void shouldRemoveExistingItem() {
        SubscribedItem testItem1 = Items.subscribedFrom("item1");
        subscribedItems.addItem(testItem1);
        assertThat(subscribedItems.size()).isEqualTo(1);
        Optional<SubscribedItem> removed = subscribedItems.removeItem("item1");
        assertThat(subscribedItems.size()).isEqualTo(0);

        assertThat(removed).hasValue(testItem1);
        assertThat(subscribedItems.getItem("item1")).isEmpty();
    }

    @Test
    public void shouldReturnNullWhenRemovingNonExistentItem() {
        Optional<SubscribedItem> removed = subscribedItems.removeItem("nonexistent");
        assertThat(removed).isEmpty();
    }

    // @Test
    // public void shouldManageSubscriptions() {
    //     SubscribedItems subscribedItems = SubscribedItems.create();
    //     assertThat(subscribedItems.acceptSubscriptions()).isTrue();
    //     assertThat(subscribedItems.isEmpty()).isTrue();

    //     SubscribedItem item1 = subscribedFrom("anItem");
    //     subscribedItems.addItem(item1);
    //     assertThat(subscribedItems).hasSize(1);
    //     assertThat(subscribedItems.getItem("anItem")).hasValue(item1);

    //     SubscribedItem item2 = subscribedFrom("anItem2");
    //     subscribedItems.addItem(item2);
    //     assertThat(subscribedItems).hasSize(2);
    //     assertThat(subscribedItems.getItem("anItem2")).hasValue(item2);

    //     Optional<SubscribedItem> removedItem1 = subscribedItems.removeItem("anItem");
    //     assertThat(removedItem1).hasValue(item1);
    //     assertThat(subscribedItems.getItem("anItem")).isEmpty();
    //     assertThat(subscribedItems).hasSize(1);

    //     Optional<SubscribedItem> removedItem2 = subscribedItems.removeItem("anItem2");
    //     assertThat(removedItem2).hasValue(item2);
    //     assertThat(subscribedItems.getItem("anItem2")).isEmpty();
    //     assertThat(subscribedItems).isEmpty();
    // }

    @Test
    public void shouldNotManageSubscriptionsFromNop() {
        SubscribedItems subscribedItems = SubscribedItems.nop();
        assertThat(subscribedItems.acceptSubscriptions()).isFalse();

        SubscribedItem item = subscribedFrom("anItem");
        subscribedItems.addItem(item);
        assertThat(subscribedItems.isEmpty()).isTrue();
        assertThat(subscribedItems.size()).isEqualTo(0);

        Optional<SubscribedItem> removedItem = subscribedItems.removeItem("anItem");
        assertThat(removedItem).isEmpty();
    }
}
