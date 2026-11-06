
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

public class DestinationMappingConfigTest {

    static Stream<Arguments> mappingFromDelimitedMappings() {
        return Stream.of(
                arguments("destination", "topic", Set.of("topic")),
                arguments("destination", "topic1,topic2", Set.of("topic1", "topic2")));
    }

    @ParameterizedTest
    @MethodSource("mappingFromDelimitedMappings")
    void shouldCreateDestinationMappingConfig(
            String destination, String delimitedTopics, Set<String> expectedTopics) {
        DestinationMappingConfig dmc =
                DestinationMappingConfig.fromDelimitedMappings(destination, delimitedTopics);
        assertThat(dmc.destination()).isEqualTo(destination);
        assertThat(dmc.topics()).isEqualTo(expectedTopics);
    }

    @Test
    void shouldCreateDestinationMappingConfigFromMap() {
        List<DestinationMappingConfig> dmc1 =
                DestinationMappingConfig.from(Map.of("destination", "topic"));
        assertThat(dmc1).hasSize(1);

        DestinationMappingConfig dmc1a = dmc1.get(0);
        assertThat(dmc1a.destination()).isEqualTo("destination");
        assertThat(dmc1a.topics()).isEqualTo(Set.of("topic"));

        List<DestinationMappingConfig> dmc2 =
                DestinationMappingConfig.from(Map.of("destination", "topic1,topic2"));
        assertThat(dmc2).hasSize(1);

        DestinationMappingConfig dmc2a = dmc2.get(0);
        assertThat(dmc2a.destination()).isEqualTo("destination");
        assertThat(dmc2a.topics()).isEqualTo(Set.of("topic1", "topic2"));

        List<DestinationMappingConfig> dmc3 =
                DestinationMappingConfig.from(
                        map("destination1", "topic1", "destination2", "topic2"));
        assertThat(dmc3).hasSize(2);

        DestinationMappingConfig dmc3a = dmc3.get(0);
        assertThat(dmc3a.destination()).isEqualTo("destination1");
        assertThat(dmc3a.topics()).isEqualTo(Set.of("topic1"));

        DestinationMappingConfig dmc3b = dmc3.get(1);
        assertThat(dmc3b.destination()).isEqualTo("destination2");
        assertThat(dmc3b.topics()).isEqualTo(Set.of("topic2"));
    }

    @Test
    void shouldCreateEmptyDestinationMappingConfig() {
        List<DestinationMappingConfig> dmc = DestinationMappingConfig.from(Map.of());
        assertThat(dmc).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromStringDueToInvalidDestination(String invalidDestination) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                DestinationMappingConfig.fromDelimitedMappings(
                                        invalidDestination, "topic"));

        assertThat(ce.getMessage()).contains("Destination must be a non-empty string");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateFromMapDueToInvalidDestination(String invalidDestination) {
        Map<String, String> map = new HashMap<>();
        map.put(invalidDestination, "topic");
        ConfigException ce =
                assertThrows(ConfigException.class, () -> DestinationMappingConfig.from(map));

        assertThat(ce.getMessage()).contains("Destination must be a non-empty string");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "topic,", // Invalid trailing ','
                "," // Generates two empty topics
            })
    void shouldNotCreateFromMapDueToInvalidTopic(String invalidTopic) {
        Map<String, String> map = new HashMap<>();
        map.put("destination", invalidTopic);
        ConfigException ce =
                assertThrows(ConfigException.class, () -> DestinationMappingConfig.from(map));

        assertThat(ce.getMessage()).contains("Destination mappings must be non-empty strings");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "topic,", // Invalid trailing ','
                "," // Generates two empty topics
            })
    void shouldNotCreateFromStringDueToInvalidTopic(String topic) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> DestinationMappingConfig.fromDelimitedMappings("destination", topic));

        assertThat(ce.getMessage()).contains("Destination mappings must be non-empty strings");
    }

    /** Ensures insertion order */
    static Map<String, String> map(String k1, String v1, String k2, String v2) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }
}
