
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
import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class DefaultSubscribedItemTest {

    @Test
    public void shouldHaveSchemaAndValues() {
        SubscribedItem item = Items.subscribedFrom("source", "item", Map.of("a", "A", "b", "B"));

        Schema schema = item.schema();
        assertThat(schema).isNotNull();
        assertThat(schema.name()).isEqualTo("item");
        assertThat(schema.keys()).containsExactly("a", "b");
        assertThat(item.values()).containsExactly("b", "B", "a", "A");
        assertThat(item.itemHandle()).isEqualTo("source");
    }

    @Test
    public void shouldSetSnapshotFlag() {
        SubscribedItem item = Items.subscribedFrom("source", "item", Map.of("a", "A", "b", "B"));
        assertThat(item.isSnapshot()).isTrue();

        item.setSnapshot(false);
        assertThat(item.isSnapshot()).isFalse();
    }

    @Test
    public void shouldBeEquals() {
        SubscribedItem item1 = Items.subscribedFrom("source", "item", Map.of("a", "A", "b", "B"));
        SubscribedItem item2 = Items.subscribedFrom("source", "item", Map.of("a", "A", "b", "B"));
        assertThat(item1).isNotSameInstanceAs(item2);
        assertThat(item1.equals(item2)).isTrue();
        assertThat(item1.hashCode()).isEqualTo(item2.hashCode());
    }

    @Test
    public void shouldNotBeEquals() {
        SubscribedItem item1 = Items.subscribedFrom("source1", "item", Map.of("a", "A", "b", "B"));
        SubscribedItem item2 = Items.subscribedFrom("source2", "item", Map.of("a", "A", "b", "B"));
        assertThat(item1.equals(item2)).isFalse();
        assertThat(item1.hashCode()).isNotEqualTo(item2.hashCode());
        assertThat(item1.equals("")).isFalse();
    }

    static Stream<Arguments> matching() {
        return Stream.of(
                arguments(Map.of("n1", "1"), Map.of("n1", "1"), List.of("n1")),
                arguments(
                        Map.of("n1", "1", "n2", "2"),
                        Map.of("n1", "1", "n2", "2"),
                        List.of("n1", "n2")));
    }

    @ParameterizedTest
    @MethodSource("matching")
    public void shouldMatch(
            Map<String, String> values1, Map<String, String> values2, List<String> expectedKeys) {
        SubscribedItem item1 = Items.subscribedFrom("source", "item", values1);
        SubscribedItem item2 = Items.subscribedFrom("source", "item", values2);
        assertThat(item1.matches(item2)).isTrue();
    }

    static Stream<Arguments> notMatching() {
        return Stream.of(
                arguments("prefix", Map.of("n1", "1"), "prefix", Map.of("n2", "2")),
                arguments(
                        "prefix",
                        Map.of("n1", "1", "n2", "2", "n3", "3"),
                        "prefix",
                        Map.of("n1", "1", "n2", "2")),
                arguments("prefix", Map.of("key", "value1"), "prefix", Map.of("key", "value2")),
                arguments(
                        "prefix1",
                        Map.of("sameKey", "sameValue"),
                        "prefix2",
                        Map.of("sameKey", "sameValue")));
    }

    @ParameterizedTest
    @MethodSource("notMatching")
    public void shouldNotMatch(
            String prefix1,
            Map<String, String> values1,
            String prefix2,
            Map<String, String> values2) {
        SubscribedItem item1 = Items.subscribedFrom("source", prefix1, values1);
        SubscribedItem item2 = Items.subscribedFrom("source", prefix2, values2);
        assertThat(item1.matches(item2)).isFalse();
    }
}
