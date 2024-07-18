
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

public class SchemaTest {

    static Stream<Set<String>> keySets() {
        return Stream.of(Set.of("key1"), Set.of("key1", "key2"));
    }

    @ParameterizedTest
    @MethodSource("keySets")
    void shouldCreate(Set<String> keys) {
        Schema schema = Schema.from("schemaName", keys);
        assertThat(schema).isNotNull();
        assertThat(schema.name()).isEqualTo("schemaName");
        assertThat(schema.keys()).isEqualTo(keys);
    }

    @ParameterizedTest
    @NullSource
    @EmptySource
    @ValueSource(strings = {"  ", "\t", "\n"})
    void shouldNotCreate(String schemaName) {
        assertThrows(IllegalArgumentException.class, () -> Schema.from(schemaName, Set.of("key")));
        assertThrows(IllegalArgumentException.class, () -> Schema.empty(schemaName));
    }

    @Test
    void shouldCreateEmptySchema() {
        Schema schema = Schema.empty("schemaName");
        assertThat(schema).isNotNull();
        assertThat(schema.name()).isEqualTo("schemaName");
        assertThat(schema.keys()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("keySets")
    void shouldMatchWithSingleKey(Set<String> keys) {
        Schema schema1 = Schema.from("schemaName", Collections.unmodifiableSet(keys));
        Schema schema2 = Schema.from("schemaName", Collections.unmodifiableSet(keys));
        assertThat(schema1.matches(schema2)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("keySets")
    void shouldNotMatchDueToDifferentNames(Set<String> keys) {
        Schema schema1 = Schema.from("schemaName1", Collections.unmodifiableSet(keys));
        Schema schema2 = Schema.from("schemaName2", Collections.unmodifiableSet(keys));
        assertThat(schema1.matches(schema2)).isFalse();
    }

    static Stream<Arguments> notMatchingKeySets() {
        return Stream.of(
                arguments(Set.of("key1"), Set.of("key2")),
                arguments(Set.of("key1", "key2"), Set.of("key1", "key3")),
                arguments(Set.of("key1"), Collections.emptySet()));
    }

    @ParameterizedTest
    @MethodSource("notMatchingKeySets")
    void shouldNotMatchDueToDifferentKeys(Set<String> keySet1, Set<String> keySet2) {
        Schema schema1 = Schema.from("schemaName", keySet1);
        Schema schema2 = Schema.from("schemaName", keySet2);
        assertThat(schema1.matches(schema2)).isFalse();
    }
}
