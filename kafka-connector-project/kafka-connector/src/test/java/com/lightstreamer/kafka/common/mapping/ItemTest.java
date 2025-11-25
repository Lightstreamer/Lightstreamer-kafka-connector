
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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ItemTest {

    static Stream<Arguments> provideData() {
        return Stream.of(
                arguments("item", Collections.emptySet(), "item"),
                arguments(
                        "item2",
                        Set.of(Data.from("b", "B"), Data.from("a", "A")),
                        "item2-[a=A,b=B]"),
                arguments("item3", Set.of(Data.from("key", "value")), "item3-[key=value]"));
    }

    @ParameterizedTest
    @MethodSource("provideData")
    public void shouldSubscribeFromSubscriptionExpression(
            String prefix, Set<Data> inputParams, String expectedNormalizedString) {
        SortedSet<Data> params = new TreeSet<>(inputParams);
        SubscribedItem item =
                Items.subscribedFrom(new SubscriptionExpression(prefix, params), new Object());

        Schema schema = item.schema();
        assertThat(schema.name()).isEqualTo(prefix);
        assertThat(schema.keys())
                .isEqualTo(inputParams.stream().map(Data::name).collect(Collectors.toSet()));
        assertThat(item.asCanonicalItemName()).isEqualTo(expectedNormalizedString);
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
    public void shouldSubscribeFromStringExpression(
            String expression,
            String expectedPrefix,
            Set<String> expectedKeys,
            String expectedNormalizedString) {
        Object handle = new Object();
        SubscribedItem item = Items.subscribedFrom(expression, handle);
        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isSameInstanceAs(handle);
        assertThat(item.schema().name()).isEqualTo(expectedPrefix);
        assertThat(item.schema().keys()).isEqualTo(expectedKeys);
        assertThat(item.asCanonicalItemName()).isEqualTo(expectedNormalizedString);

        SubscribedItem item2 = Items.subscribedFrom(expression);
        assertThat(item2).isNotNull();
        assertThat(item2.itemHandle()).isSameInstanceAs(expression);
        assertThat(item2.schema().name()).isEqualTo(expectedPrefix);
        assertThat(item2.schema().keys()).isEqualTo(expectedKeys);
        assertThat(item2.asCanonicalItemName()).isEqualTo(expectedNormalizedString);
    }

    static Stream<Arguments> provideEqualValues() {
        return Stream.of(
                arguments(
                        List.of(Data.from("n1", "1")),
                        List.of(Data.from("n1", "1")),
                        List.of("n1")),
                arguments(
                        List.of(Data.from("n1", "1"), Data.from("n2", "2")),
                        List.of(Data.from("n1", "1"), Data.from("n2", "2")),
                        List.of("n1", "n2")));
    }

    @ParameterizedTest
    @MethodSource("provideEqualValues")
    public void shouldSubscribeToEqualItems(
            List<Data> values1, List<Data> values2, List<String> expectedKeys) {
        Object itemHandle = new Object();
        SubscribedItem item1 =
                Items.subscribedFrom(
                        new SubscriptionExpression("item", new TreeSet<>(values1)), itemHandle);
        SubscribedItem item2 =
                Items.subscribedFrom(
                        new SubscriptionExpression("item", new TreeSet<>(values2)), itemHandle);
        assertThat(item1.equals(item2)).isTrue();
    }

    static Stream<Arguments> provideNotEqualItems() {
        return Stream.of(
                arguments(List.of(Data.from("n1", "1")), List.of(Data.from("n2", "2"))),
                arguments(
                        List.of(Data.from("n1", "1"), Data.from("n2", "2"), Data.from("n3", "3")),
                        List.of(Data.from("n1", "1"), Data.from("n2", "2"))),
                arguments(
                        List.of(Data.from("key", "value1")), List.of(Data.from("key", "value2"))));
    }

    @ParameterizedTest
    @MethodSource("provideNotEqualItems")
    public void shouldSubscribeToNotEqualItems(List<Data> values1, List<Data> values2) {
        Object itemHandle = new Object();
        SubscribedItem item1 =
                Items.subscribedFrom(
                        new SubscriptionExpression("prefix", new TreeSet<>(values1)), itemHandle);
        SubscribedItem item2 =
                Items.subscribedFrom(
                        new SubscriptionExpression("prefix", new TreeSet<>(values2)), itemHandle);
        assertThat(item1.equals(item2)).isFalse();
    }

    @Test
    public void shouldSubscribeToNotEqualItemsDueToDifferentPrefix() {
        List<Data> sameValues = List.of(Data.from("n1", "1"));
        Object itemHandle = new Object();
        SubscribedItem item1 =
                Items.subscribedFrom(
                        new SubscriptionExpression("aPrefix", new TreeSet<>(sameValues)),
                        itemHandle);
        SubscribedItem item2 =
                Items.subscribedFrom(
                        new SubscriptionExpression("anotherPrefix", new TreeSet<>(sameValues)),
                        itemHandle);
        assertThat(item1.equals(item2)).isFalse();
    }
}
