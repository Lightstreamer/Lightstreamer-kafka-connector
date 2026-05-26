
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Subscription;

import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class OnDemandSubscribedItemsTest {

    private OnDemandSubscribedItems subscribedItems;

    @BeforeEach
    public void setUp() {
        this.subscribedItems = SubscribedItems.onDemand();
    }

    @Test
    public void shouldBeEmptyOnCreation() {
        assertThat(subscribedItems.isEmpty()).isTrue();
        assertThat(subscribedItems.size()).isEqualTo(0);
    }

    @Test
    public void shouldAddAndRetrieveSimpleItems() {
        OnDemandSubscribedItem testItem1 =
                Items.onDemandSubscribedFrom(Subscription("item1"), new Object());
        OnDemandSubscribedItem testItem2 =
                Items.onDemandSubscribedFrom(Subscription("item2"), new Object());
        subscribedItems.addItem(testItem1);
        assertThat(subscribedItems.size()).isEqualTo(1);
        assertThat(subscribedItems.isEmpty()).isFalse();

        subscribedItems.addItem(testItem2);
        assertThat(subscribedItems.size()).isEqualTo(2);

        assertThat(subscribedItems.getItem("item1")).isSameInstanceAs(testItem1);
        assertThat(subscribedItems.getItem("item2")).isSameInstanceAs(testItem2);

        List<SubscribedItem> items = new ArrayList<>();
        subscribedItems.forEach(items::add);

        assertThat(items).containsExactly(testItem1, testItem2);
    }

    @Test
    public void shouldAddAndRetrieveCanonicalItems() {
        OnDemandSubscribedItem testItem1 =
                Items.onDemandSubscribedFrom(Subscription("item-[b=2,a=1]"), new Object());
        subscribedItems.addItem(testItem1);

        // Retrieve the item from its canonical representation
        assertThat(subscribedItems.getItem("item-[a=1,b=2]")).isSameInstanceAs(testItem1);
    }

    @Test
    public void shouldReplaceItemWhenAddingDuplicate() {
        OnDemandSubscribedItem testItem1 =
                Items.onDemandSubscribedFrom(Subscription("item1"), new Object());
        OnDemandSubscribedItem testItem1Duplicate =
                Items.onDemandSubscribedFrom(Subscription("item1"), new Object());

        subscribedItems.addItem(testItem1);
        assertThat(subscribedItems.size()).isEqualTo(1);
        subscribedItems.addItem(testItem1Duplicate);
        assertThat(subscribedItems.size()).isEqualTo(1);

        assertThat(subscribedItems.getItem("item1")).isSameInstanceAs(testItem1Duplicate);

        List<SubscribedItem> items = new ArrayList<>();
        subscribedItems.forEach(items::add);

        assertThat(items).containsExactly(testItem1Duplicate);
    }

    @Test
    public void shouldReturnNullForNonExistentItem() {
        assertThat(subscribedItems.getItem("nonexistent")).isNull();
    }

    @Test
    public void shouldRemoveExistingItem() {
        OnDemandSubscribedItem testItem1 =
                Items.onDemandSubscribedFrom(Subscription("item1"), new Object());
        subscribedItems.addItem(testItem1);
        assertThat(subscribedItems.size()).isEqualTo(1);
        Optional<SubscribedItem> removed = subscribedItems.removeItem("item1");
        assertThat(subscribedItems.size()).isEqualTo(0);
        assertThat(subscribedItems.isEmpty()).isTrue();

        assertThat(removed).hasValue(testItem1);
        assertThat(subscribedItems.getItem("item1")).isNull();

        List<SubscribedItem> items = new ArrayList<>();
        subscribedItems.forEach(items::add);

        assertThat(items).isEmpty();
    }

    @Test
    public void shouldReturnNullWhenRemovingNonExistentItem() {
        Optional<SubscribedItem> removed = subscribedItems.removeItem("nonexistent");
        assertThat(removed).isEmpty();
    }
}
