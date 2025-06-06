
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
                arguments("topic", "item", Set.of("item")),
                arguments("topic", "item ", Set.of("item")),
                arguments("topic", "item1,item2", Set.of("item1", "item2")),
                arguments("topic", " item1 ,  item2  ", Set.of("item1", "item2")),
                arguments("topic", "sameItem,sameItem", Set.of("sameItem")));
    }

    @ParameterizedTest
    @MethodSource("mappingFromDelimitedString")
    void shouldCreateTopicMappingFromDelimitedString(
            String topic, String delimitedItems, Set<String> expectedItems) {
        TopicMappingConfig tm = TopicMappingConfig.fromDelimitedMappings(topic, delimitedItems);
        assertThat(tm.topic()).isEqualTo(topic);
        assertThat(tm.mappings()).isEqualTo(expectedItems);
    }

    /** Ensures insertion order */
    static Map<String, String> map(String k1, String v1, String k2, String v2) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    @Test
    void shouldCreateTopicMappingFromMap() {
        Map<String, String> configs1 = Map.of("topic", "item1");
        List<TopicMappingConfig> tms1 = TopicMappingConfig.from(configs1);
        assertThat(tms1).hasSize(1);

        TopicMappingConfig tms1a = tms1.get(0);
        assertThat(tms1a.topic()).isEqualTo("topic");
        assertThat(tms1a.mappings()).containsExactly("item1");

        Map<String, String> configs2 = Map.of("topic", "item1,item2");
        List<TopicMappingConfig> tms2 = TopicMappingConfig.from(configs2);
        assertThat(tms2).hasSize(1);

        TopicMappingConfig tms2a = tms2.get(0);
        assertThat(tms2a.topic()).isEqualTo("topic");
        assertThat(tms2a.mappings()).containsExactly("item1", "item2");

        Map<String, String> configs3 = Map.of("topic1", "sameItem,sameItem");
        List<TopicMappingConfig> tms3 = TopicMappingConfig.from(configs3);
        assertThat(tms3).hasSize(1);

        TopicMappingConfig tms3a = tms3.get(0);
        assertThat(tms3a.topic()).isEqualTo("topic1");
        assertThat(tms3a.mappings()).containsExactly("sameItem");

        Map<String, String> configs4 = map("topic1", "item1a,item1b", "topic2", "item2a,item2b");
        List<TopicMappingConfig> tms4 = TopicMappingConfig.from(configs4);
        assertThat(tms4).hasSize(2);

        TopicMappingConfig tms4a = tms4.get(0);
        assertThat(tms4a.topic()).isEqualTo("topic1");
        assertThat(tms4a.mappings()).containsExactly("item1a", "item1b");

        TopicMappingConfig tms3b = tms4.get(1);
        assertThat(tms3b.topic()).isEqualTo("topic2");
        assertThat(tms3b.mappings()).containsExactly("item2a", "item2b");
    }

    @Test
    void shouldCreateEmptyTopicMappingListFromEmptyMap() {
        Map<String, String> map = new HashMap<>();
        List<TopicMappingConfig> from = TopicMappingConfig.from(map);
        assertThat(from).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromMapDueToInvalidTopic(String topic) {
        Map<String, String> map = new HashMap<>();
        map.put(topic, null);
        ConfigException ce =
                assertThrows(ConfigException.class, () -> TopicMappingConfig.from(map));
        assertThat(ce.getMessage()).isEqualTo("Topic must be a non-empty string");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromStringDueToInvalidTopic(String topic) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> TopicMappingConfig.fromDelimitedMappings(topic, "item"));
        assertThat(ce.getMessage()).isEqualTo("Topic must be a non-empty string");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "item,", // Invalid trailing ','
                ",", // Generates two empty strings
            })
    void shouldNotCreateFromMapDueToInvalidMapping(String mapping) {
        Map<String, String> map = new HashMap<>();
        map.put("topic", mapping);
        ConfigException ce =
                assertThrows(ConfigException.class, () -> TopicMappingConfig.from(map));
        assertThat(ce.getMessage()).isEqualTo("Topic mappings must be non-empty strings");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromStringDueToInvalidMapping(String mapping) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> TopicMappingConfig.fromDelimitedMappings("topic", mapping));
        assertThat(ce.getMessage()).isEqualTo("Topic mappings must be non-empty strings");
    }
}
