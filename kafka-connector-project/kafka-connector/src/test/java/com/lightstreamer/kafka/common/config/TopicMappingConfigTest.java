
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

package com.lightstreamer.kafka.common.config;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class TopicMappingConfigTest {

    static Stream<Arguments> mappingFromDelimitedString() {
        return Stream.of(
                arguments("topic", "item", "", Set.of("item"), Set.of()),
                arguments("topic", "item", null, Set.of("item"), Set.of()),
                arguments("topic", "item ", "", Set.of("item"), Set.of()),
                arguments("topic", "item ", "0,1,2,3", Set.of("item"), Set.of(0, 1, 2, 3)),
                arguments("topic", "item ", "0-3", Set.of("item"), Set.of(0, 1, 2, 3)),
                arguments("topic", "item ", "0 - 3, 1, 2  ", Set.of("item"), Set.of(0, 1, 2, 3)),
                arguments("topic", "item ", "0-3, 1-3", Set.of("item"), Set.of(0, 1, 2, 3)),
                arguments("topic", "item ", "0-2,3- 5", Set.of("item"), Set.of(0, 1, 2, 3, 4, 5)),
                arguments("topic", "item1,item2", "", Set.of("item1", "item2"), Set.of()),
                arguments(
                        "topic",
                        "item1,item2",
                        "4-6,1,2",
                        Set.of("item1", "item2"),
                        Set.of(1, 2, 4, 5, 6)),
                arguments("topic", " item1 ,  item2  ", "", Set.of("item1", "item2"), Set.of()),
                arguments("topic", "sameItem,sameItem", "", Set.of("sameItem"), Set.of()));
    }

    @ParameterizedTest
    @MethodSource("mappingFromDelimitedString")
    void shouldCreateTopicMappingFromDelimitedString(
            String topic,
            String delimitedItems,
            String delimitedPartitions,
            Set<String> expectedItems,
            Set<Integer> expectedPartitions) {
        TopicMappingConfig tm =
                TopicMappingConfig.fromDelimitedMappings(
                        topic, delimitedItems, delimitedPartitions);
        assertThat(tm.topic()).isEqualTo(topic);
        assertThat(tm.mappings()).isEqualTo(expectedItems);
        assertThat(tm.partitions()).isEqualTo(expectedPartitions);
    }

    /** Ensures insertion order */
    static Map<String, String> map(String k1, String v1, String k2, String v2) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    @Test
    void shouldCreateTopicMappingFromMaps() {
        List<TopicMappingConfig> tms1 = TopicMappingConfig.from(Map.of("topic", "item1"));
        assertThat(tms1).hasSize(1);

        TopicMappingConfig tms1a = tms1.get(0);
        assertThat(tms1a.topic()).isEqualTo("topic");
        assertThat(tms1a.mappings()).containsExactly("item1");
        assertThat(tms1a.partitions()).isEmpty();

        List<TopicMappingConfig> tms2 =
                TopicMappingConfig.from(Map.of("topic", "item1,item2"), Map.of("topic", "0-4,6-8"));
        assertThat(tms2).hasSize(1);

        TopicMappingConfig tms2a = tms2.get(0);
        assertThat(tms2a.topic()).isEqualTo("topic");
        assertThat(tms2a.mappings()).containsExactly("item1", "item2");
        assertThat(tms2a.partitions()).isEqualTo(Set.of(0, 1, 2, 3, 4, 6, 7, 8));

        List<TopicMappingConfig> tms3 =
                TopicMappingConfig.from(
                        Map.of("topic1", "sameItem,sameItem"), Map.of("topic1", "1,2"));
        assertThat(tms3).hasSize(1);

        TopicMappingConfig tms3a = tms3.get(0);
        assertThat(tms3a.topic()).isEqualTo("topic1");
        assertThat(tms3a.mappings()).containsExactly("sameItem");
        assertThat(tms3a.partitions()).isEqualTo(Set.of(1, 2));

        List<TopicMappingConfig> tms4 =
                TopicMappingConfig.from(
                        map("topic1", "item1a,item1b", "topic2", "item2a,item2b"),
                        Map.of("topic1", "0-2", "topic2", "3-5"));
        assertThat(tms4).hasSize(2);

        TopicMappingConfig tms4a = tms4.get(0);
        assertThat(tms4a.topic()).isEqualTo("topic1");
        assertThat(tms4a.mappings()).containsExactly("item1a", "item1b");
        assertThat(tms4a.partitions()).isEqualTo(Set.of(0, 1, 2));

        TopicMappingConfig tms4b = tms4.get(1);
        assertThat(tms4b.topic()).isEqualTo("topic2");
        assertThat(tms4b.mappings()).containsExactly("item2a", "item2b");
        assertThat(tms4b.partitions()).isEqualTo(Set.of(3, 4, 5));
    }

    @Test
    void shouldCreateEmptyTopicMappingListFromEmptyMap() {
        List<TopicMappingConfig> from = TopicMappingConfig.from(Map.of(), Map.of());
        assertThat(from).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromMapDueToInvalidTopic(String topic) {
        Map<String, String> map = new HashMap<>();
        map.put(topic, null);
        ConfigException ce =
                assertThrows(ConfigException.class, () -> TopicMappingConfig.from(map));
        assertThat(ce).hasMessageThat().isEqualTo("Topic must be a non-empty string");
    }

    @Test
    void shouldNotCreateFromMapDueToInvalidPartitionMapping() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                TopicMappingConfig.from(
                                        Map.of("topic", "item"), Map.of("anotherTopic", "0-3")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Partition mappings found for topics with no item mappings: [anotherTopic]");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromStringDueToInvalidTopic(String topic) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> TopicMappingConfig.fromDelimitedMappings(topic, "item", ""));
        assertThat(ce).hasMessageThat().isEqualTo("Topic must be a non-empty string");
    }

    static Stream<Arguments> invalidPartitionMappings() {
        return Stream.of(
                arguments("-0", "Partition numbers must be non-negative: [-0]"),
                arguments("A-B", "Partition range bounds must be integers: [A-B]"),
                arguments("0-", "Partition range bounds must be integers: [0-]"),
                arguments("0-3,1-2,5-4", "Partition range start must be <= end: [5-4]"),
                arguments("0-3,1-2,5-4,7-6", "Partition range start must be <= end: [5-4]"),
                arguments("0--3", "Partition range bounds must be integers: [0--3]"),
                arguments("1.5", "Partition range bounds must be integers: [1.5]"),
                arguments("0-3,1-2,5-4,7-6,9-8", "Partition range start must be <= end: [5-4]"));
    }

    @ParameterizedTest
    @MethodSource("invalidPartitionMappings")
    void shouldNotCreateFromStringDueToInvalidPartitions(
            String partitions, String expectedMessage) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                TopicMappingConfig.fromDelimitedMappings(
                                        "topic", "item", partitions));
        assertThat(ce).hasMessageThat().isEqualTo(expectedMessage);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "item,", // Invalid trailing ','
                ",", // Generates two empty strings
            })
    void shouldNotCreateFromMapDueToInvalidTopicMapping(String mapping) {
        Map<String, String> map = new HashMap<>();
        map.put("topic", mapping);
        ConfigException ce =
                assertThrows(ConfigException.class, () -> TopicMappingConfig.from(map));
        assertThat(ce).hasMessageThat().isEqualTo("Topic mappings must be non-empty strings");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromStringDueToInvalidTopicMapping(String mapping) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> TopicMappingConfig.fromDelimitedMappings("topic", mapping, ""));
        assertThat(ce).hasMessageThat().isEqualTo("Topic mappings must be non-empty strings");
    }
}
