
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

package com.lightstreamer.kafka.common.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Test;

import java.util.Set;

public class SchemaTest {

    @Test
    public void shouldCreateSchema() {
        Schema schema = Schema.from("test", Set.of("a", "b"));

        assertThat(schema.name()).isEqualTo("test");
        assertThat(schema.keys()).containsExactly("a", "b");
    }

    @Test
    public void shouldCreateEmptySchema() {
        Schema schema = Schema.empty("test");

        assertThat(schema.name()).isEqualTo("test");
        assertThat(schema.keys()).isEmpty();
    }

    @Test
    public void shouldCreateNopSchema() {
        Schema schema = Schema.nop();

        assertThat(schema.name()).isEqualTo("NOSCHEMA");
        assertThat(schema.keys()).isEmpty();
    }

    @Test
    public void shouldMatch() {
        Schema schema1 = Schema.from("test", Set.of("a", "b"));
        Schema schema2 = Schema.from("test", Set.of("a", "b"));

        assertThat(schema1.matches(schema2)).isTrue();
    }

    @Test
    public void shouldNotMatchDifferentName() {
        Schema schema1 = Schema.from("test1", Set.of("a", "b"));
        Schema schema2 = Schema.from("test2", Set.of("a", "b"));

        assertThat(schema1.matches(schema2)).isFalse();
    }

    @Test
    public void shouldNotMatchDifferentKeys() {
        Schema schema1 = Schema.from("test", Set.of("a", "b"));
        Schema schema2 = Schema.from("test", Set.of("a", "c"));

        assertThat(schema1.matches(schema2)).isFalse();
    }

    @Test
    public void shouldThrowOnNullName() {
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            Schema.from(null, Set.of("a", "b"));
                        });
        assertThat(exception.getMessage()).isEqualTo("Schema name must be a non empty string");
    }

    @Test
    public void shouldThrowOnBlankName() {
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> {
                            Schema.from("  ", Set.of("a", "b"));
                        });
        assertThat(exception.getMessage()).isEqualTo("Schema name must be a non empty string");
    }
}
