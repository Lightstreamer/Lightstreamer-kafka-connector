
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

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SubscribeItemsTest {

    private SubscribedItems subscribedItems;

    @BeforeEach
    public void setUp() {
        subscribedItems = SubscribedItems.create();
    }

    @Test
    public void shouldAddAndRetrieveSimpleItems() {
        SubscribedItem testItem1 = Items.subscribedFrom("item1");
        SubscribedItem testItem2 = Items.subscribedFrom("item2");
        subscribedItems.add(testItem1);
        subscribedItems.add(testItem2);

        assertThat(subscribedItems.get("item1")).isEqualTo(testItem1);
        assertThat(subscribedItems.get("item2")).isEqualTo(testItem2);
    }

    @Test
    public void shouldAddAndRetrieveCanonicalItems() {
        SubscribedItem testItem1 = Items.subscribedFrom("item-[b=2,a=1]");
        subscribedItems.add(testItem1);

        // Retrieve the item from its canonical representation
        assertThat(subscribedItems.get("item-[a=1,b=2]")).isEqualTo(testItem1);
    }

    @Test
    public void shouldReplaceItemWhenAddingDuplicate() {
        SubscribedItem testItem1 = Items.subscribedFrom("item1");
        SubscribedItem testItem1Duplicate = Items.subscribedFrom("item1");

        subscribedItems.add(testItem1);
        subscribedItems.add(testItem1Duplicate);

        assertThat(subscribedItems.get("item1")).isEqualTo(testItem1Duplicate);
    }

    @Test
    public void shouldReturnNullForNonExistentItem() {
        assertThat(subscribedItems.get("nonexistent")).isNull();
    }

    @Test
    public void shouldRemoveExistingItem() {
        SubscribedItem testItem1 = Items.subscribedFrom("item1");
        subscribedItems.add(testItem1);
        SubscribedItem removed = subscribedItems.remove("item1");

        assertThat(removed).isEqualTo(testItem1);
        assertThat(subscribedItems.get("item1")).isNull();
    }

    @Test
    public void shouldReturnNullWhenRemovingNonExistentItem() {
        SubscribedItem removed = subscribedItems.remove("nonexistent");
        assertThat(removed).isNull();
    }
}
