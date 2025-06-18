
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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public class SubscribedItemsTest {

    static Stream<Arguments> items() {
        return Stream.of(
                arguments(Collections.emptySet(), 0),
                arguments(Set.of(Items.subscribedFrom("anItem")), 1),
                arguments(
                        Set.of(Items.subscribedFrom("anItem"), Items.subscribedFrom("anotherItem")),
                        2));
    }

    @ParameterizedTest
    @MethodSource("items")
    public void shouldCreate(Collection<SubscribedItem> items, int expectedSize) {
        SubscribedItems subscribedItems = SubscribedItems.of(items);
        assertThat(subscribedItems.isNop()).isFalse();
        int counter = 0;
        Iterator<SubscribedItem> iterator = subscribedItems.iterator();
        while (iterator.hasNext()) {
            counter++;
            iterator.next();
        }
        assertThat(counter).isEqualTo(expectedSize);
    }

    @Test
    public void shouldCreateNop() {
        SubscribedItems subscribedItems = SubscribedItems.nop();
        assertThat(subscribedItems.iterator().hasNext()).isFalse();
        assertThat(subscribedItems.isNop()).isTrue();
    }
}
