
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.test_utils.Mocks.EventCall;
import com.lightstreamer.kafka.test_utils.Mocks.MockItemEventListener;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

public class OnDemandSubscribedItemTest {

    @Test
    public void shouldCreateOnDemandSubscribedItemFromFactory() {
        String expression = "item-[name=field1]";
        Object handle = new Object();
        OnDemandSubscribedItem item = Items.onDemandSubscribedItem(expression, handle);
        assertThat(item).isNotNull();
        assertThat(item.schema().name()).isEqualTo("item");
        assertThat(item.schema().keys()).containsExactly("name");
        assertThat(item.canonicalName()).isEqualTo("item-[name=field1]");
    }

    @Test
    public void shouldNotCreateOnDemandSubscribedItemWithNullHandleFromFactory() {
        String expression = "item-[name=field1]";
        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () -> Items.onDemandSubscribedItem(expression, null));
        assertThat(exception).hasMessageThat().contains("itemHandle");
    }

    static Stream<Arguments> provideExpressions() {
        return Stream.of(
                arguments("item", "item", Collections.emptySet(), "item"),
                arguments("item-first", "item-first", Collections.emptySet(), "item-first"),
                arguments("item_123_", "item_123_", Collections.emptySet(), "item_123_"),
                arguments("item-", "item-", Collections.emptySet(), "item-"),
                arguments("prefix-[]", "prefix", Collections.emptySet(), "prefix"),
                arguments("item-[name=field1]", "item", Set.of("name"), "item-[name=field1]"),
                arguments(
                        "item-[name2=field2,name1=field1]",
                        "item",
                        Set.of("name2", "name1"),
                        "item-[name1=field1,name2=field2]"),
                arguments(
                        "item-first-[height=12.34]",
                        "item-first",
                        Set.of("height"),
                        "item-first-[height=12.34]"),
                arguments(
                        "item_123_-[test=\\]", "item_123_", Set.of("test"), "item_123_-[test=\\]"),
                arguments("item-[test=\"\"]", "item", Set.of("test"), "item-[test=\"\"]"),
                arguments("prefix-[test=]]", "prefix", Set.of("test"), "prefix-[test=]]"),
                arguments("item-[test=value,]", "item", Set.of("test"), "item-[test=value]"));
    }

    @ParameterizedTest
    @MethodSource("provideExpressions")
    public void shouldCreateOnDemandSubscribedItem(
            String expression,
            String expectedPrefix,
            Set<String> expectedKeys,
            String expectedCanonicalItemName) {
        Object handle = new Object();
        OnDemandSubscribedItem item =
                new OnDemandSubscribedItem(Expressions.Subscription(expression), handle);
        assertThat(item).isNotNull();
        assertThat(item.schema().name()).isEqualTo(expectedPrefix);
        assertThat(item.schema().keys()).isEqualTo(expectedKeys);
        assertThat(item.canonicalName()).isEqualTo(expectedCanonicalItemName);
        assertThat(item.equals(item)).isTrue();
    }

    @Test
    public void shouldNotCreateOnDemandSubscribedItemWithNullHandle() {
        String expression = "item-[name=field1]";
        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                new OnDemandSubscribedItem(
                                        Expressions.Subscription(expression), null));
        assertThat(exception).hasMessageThat().contains("itemHandle");
    }

    static Stream<Arguments> provideEqualData() {
        return Stream.of(
                arguments(List.of(Data.from("n1", "1")), List.of(Data.from("n1", "1"))),
                arguments(
                        List.of(Data.from("n1", "1"), Data.from("n2", "2")),
                        List.of(Data.from("n1", "1"), Data.from("n2", "2"))));
    }

    @ParameterizedTest
    @MethodSource("provideEqualData")
    public void shouldCreateEqualSubscribedItems(List<Data> values1, List<Data> values2) {
        Object itemHandle = new Object();
        OnDemandSubscribedItem item1 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("item", new TreeSet<>(values1)), itemHandle);
        OnDemandSubscribedItem item2 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("item", new TreeSet<>(values2)), itemHandle);
        assertThat(item1.equals(item2)).isTrue();
    }

    static Stream<Arguments> provideNotEqualData() {
        return Stream.of(
                arguments(List.of(Data.from("n1", "1")), List.of(Data.from("n2", "2"))),
                arguments(
                        List.of(Data.from("n1", "1"), Data.from("n2", "2"), Data.from("n3", "3")),
                        List.of(Data.from("n1", "1"), Data.from("n2", "2"))),
                arguments(
                        List.of(Data.from("key", "value1")), List.of(Data.from("key", "value2"))));
    }

    @ParameterizedTest
    @MethodSource("provideNotEqualData")
    public void shouldCreateNotEqualSubscribedItems(List<Data> values1, List<Data> values2) {
        Object itemHandle = new Object();
        OnDemandSubscribedItem item1 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("prefix", new TreeSet<>(values1)), itemHandle);
        OnDemandSubscribedItem item2 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("prefix", new TreeSet<>(values2)), itemHandle);
        assertThat(item1.equals(item2)).isFalse();
    }

    @Test
    public void shouldCreateNotEqualSubscribedItemsDueToDifferentPrefixes() {
        List<Data> sameValues = List.of(Data.from("n1", "1"));
        Object itemHandle = new Object();
        OnDemandSubscribedItem item1 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("aPrefix", new TreeSet<>(sameValues)),
                        itemHandle);
        OnDemandSubscribedItem item2 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("anotherPrefix", new TreeSet<>(sameValues)),
                        itemHandle);
        assertThat(item1.equals(item2)).isFalse();
    }

    @Test
    public void shouldCreateNotEqualSubscribedItemsDueToDifferentHandles() {
        List<Data> sameValues = List.of(Data.from("n1", "1"));
        Object itemHandle1 = new Object();
        Object itemHandle2 = new Object();
        OnDemandSubscribedItem item1 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("aPrefix", new TreeSet<>(sameValues)),
                        itemHandle1);
        OnDemandSubscribedItem item2 =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("aPrefix", new TreeSet<>(sameValues)),
                        itemHandle2);
        assertThat(item1.equals(item2)).isFalse();
    }

    @Test
    public void shouldDeliverEventsCorrectly() {
        MockItemEventListener eventListener = new MockItemEventListener();
        Object itemHandle = new Object();
        OnDemandSubscribedItem subscribedItem =
                new OnDemandSubscribedItem(
                        new SubscriptionExpression("item", new TreeSet<>()), itemHandle);

        Map<String, String> s1 = Map.of("field1", "snapshot1");
        Map<String, String> r1 = Map.of("field1", "realTime1");
        Map<String, String> s2 = Map.of("field1", "snapshot2");
        Map<String, String> r2 = Map.of("field1", "realTime2");

        subscribedItem.clearSnapshot(eventListener);
        subscribedItem.sendEvent(s1, eventListener, true);
        subscribedItem.endOfSnapshot(eventListener);
        subscribedItem.sendEvent(r1, eventListener, false);
        subscribedItem.sendEvent(s2, eventListener, true);
        subscribedItem.sendEvent(r2, eventListener, false);
        subscribedItem.clearSnapshot(eventListener);
        subscribedItem.clearSnapshot(eventListener);
        subscribedItem.endOfSnapshot(eventListener);

        List<EventCall> allEvents = eventListener.getEvents();
        assertThat(allEvents).hasSize(9);

        EventCall eventCall = allEvents.get(0);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.CS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);

        eventCall = allEvents.get(1);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(s1);
        assertThat(eventCall.isSnapshot()).isTrue();

        eventCall = allEvents.get(2);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.EOS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);

        eventCall = allEvents.get(3);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(r1);
        assertThat(eventCall.isSnapshot()).isFalse();

        eventCall = allEvents.get(4);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(s2);
        assertThat(eventCall.isSnapshot()).isTrue();

        eventCall = allEvents.get(5);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.UPDATE);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
        assertThat(eventCall.event()).isEqualTo(r2);
        assertThat(eventCall.isSnapshot()).isFalse();

        eventCall = allEvents.get(6);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.CS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);

        eventCall = allEvents.get(7);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.CS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);

        eventCall = allEvents.get(8);
        assertThat(eventCall.type()).isEqualTo(EventCall.EventType.EOS);
        assertThat(eventCall.handle()).isEqualTo(itemHandle);
    }

    @Test
    public void shouldMaintainSnapshotFlagBehavior() {
        String expression = "item-[name=field1]";
        Object handle = new Object();
        OnDemandSubscribedItem subscribedItem = Items.onDemandSubscribedItem(expression, handle);

        // Initially in snapshot mode
        assertThat(subscribedItem.isSnapshot()).isTrue();

        // Change flag
        subscribedItem.setSnapshot(false);
        assertThat(subscribedItem.isSnapshot()).isFalse();

        // Change back
        subscribedItem.setSnapshot(true);
        assertThat(subscribedItem.isSnapshot()).isTrue();
    }
}
