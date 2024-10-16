
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class SchemaAndValuesTest {

    private Schema schema;
    private DefaultSchemaAndValues schemaAndValues;

    @BeforeEach
    void beforeEach() {
        schema = Schema.from("schema", Set.of("key", "value"));
        schemaAndValues =
                new DefaultSchemaAndValues(schema, Map.of("key", "aKey", "value", "aValue"));
    }

    @Test
    public void shouldCreateSchemaAndValues() {
        assertThat(schemaAndValues.schema()).isSameInstanceAs(schema);
        assertThat(schemaAndValues.values()).containsExactly("key", "aKey", "value", "aValue");
    }

    @Test
    public void shouldMatch() {
        Schema anotherSchema = Schema.from("schema", Set.of("key", "value"));
        SchemaAndValues anotherSchemaAndValues =
                new DefaultSchemaAndValues(anotherSchema, Map.of("key", "aKey", "value", "aValue"));
        assertThat(schemaAndValues.matches(anotherSchemaAndValues)).isTrue();
    }

    @Test
    public void shouldNotMatchDueToDifferentNames() {
        Schema otherSchema = Schema.from("anotherSchema", Set.of("key", "value"));
        SchemaAndValues otherSchemaAndValues =
                new DefaultSchemaAndValues(otherSchema, Map.of("key", "aKey", "value", "aValue"));
        assertThat(schemaAndValues.matches(otherSchemaAndValues)).isFalse();
    }

    @Test
    public void shouldNotMatchDueToDifferentValues() {
        Schema otherSchema = schema;
        SchemaAndValues anotherSchemaAndValues =
                new DefaultSchemaAndValues(
                        otherSchema, Map.of("key=anoterKey", "value=aNotherValue"));
        assertThat(schemaAndValues.matches(anotherSchemaAndValues)).isFalse();
    }
}
