
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class DestinationConfigurationsTest {

    static Stream<Arguments> mappingFromDelimitedMappings() {
        return Stream.of(
                arguments(Map.of("destination", "topic"), "destination", Set.of("topic")),
                arguments(
                        Map.of("destination", "topic1,topic2"),
                        "destination",
                        Set.of("topic1", "topic2")));
    }

    @ParameterizedTest
    @MethodSource("mappingFromDelimitedMappings")
    void shouldCreateDestinationConfigurations(
            Map<String, String> destinationToTopics,
            String destination,
            Set<String> expectedTopics) {
        DestinationConfigurations dmc = DestinationConfigurations.from(destinationToTopics);
        assertThat(dmc.lookupTopics(destination)).isEqualTo(expectedTopics);
    }

    @Test
    void shouldCreateDestinationConfigurations() {
        DestinationConfigurations dmc1 =
                DestinationConfigurations.from(Map.of("destination", "topic"));
        assertThat(dmc1.size()).isEqualTo(1);
        assertThat(dmc1.lookupTopics("destination")).isEqualTo(Set.of("topic"));

        DestinationConfigurations dmc2 =
                DestinationConfigurations.from(Map.of("destination", "topic1,topic2"));
        assertThat(dmc2.lookupTopics("destination")).isEqualTo(Set.of("topic1", "topic2"));
        assertThat(dmc2.size()).isEqualTo(1);

        DestinationConfigurations dmc3 =
                DestinationConfigurations.from(
                        map("destination1", "topic1", "destination2", "topic2"));
        assertThat(dmc3.lookupTopics("destination1")).isEqualTo(Set.of("topic1"));
        assertThat(dmc3.lookupTopics("destination2")).isEqualTo(Set.of("topic2"));
        assertThat(dmc3.size()).isEqualTo(2);
    }

    @Test
    void shouldCreateEmptyDestinationConfigurations() {
        DestinationConfigurations dmc = DestinationConfigurations.from(Map.of());
        assertThat(dmc.size()).isEqualTo(0);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldNotCreateDueToInvalidDestination(String invalidDestination) {
        Map<String, String> map = new HashMap<>();
        map.put(invalidDestination, "topic");
        ConfigException ce =
                assertThrows(ConfigException.class, () -> DestinationConfigurations.from(map));

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
                assertThrows(ConfigException.class, () -> DestinationConfigurations.from(map));

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
