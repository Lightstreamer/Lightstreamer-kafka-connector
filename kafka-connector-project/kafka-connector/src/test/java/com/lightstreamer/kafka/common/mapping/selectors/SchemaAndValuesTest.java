
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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SchemaAndValuesTest {

    @Test
    public void shouldCreateSchemaAndValues() {
        Schema schema = Schema.from("schema", Set.of("key", "value"));
        SchemaAndValues schemaAndValues =
                SchemaAndValues.from(schema, Map.of("key", "aKey", "value", "aValue"));
        assertThat(schemaAndValues.schema()).isSameInstanceAs(schema);
        assertThat(schemaAndValues.values()).containsExactly("key", "aKey", "value", "aValue");
    }

    @Test
    public void shouldCreateSchemaAndValuesFromNameAndValues() {
        SchemaAndValues schemaAndValues =
                SchemaAndValues.from("schema", Map.of("key", "aKey", "value", "aValue"));
        assertThat(schemaAndValues.schema().name()).isEqualTo("schema");
        assertThat(schemaAndValues.schema().keys()).containsExactly("key", "value");
        assertThat(schemaAndValues.values()).containsExactly("key", "aKey", "value", "aValue");
    }

    @Test
    public void shouldMatchWhenSameSchemaAndValues() {
        Schema schema1 = Schema.from("schema1", Set.of("field1", "field2"));
        Schema schema2 = Schema.from("schema2", Set.of("field1", "field2"));
        Map<String, String> values = new HashMap<>(Map.of("field1", "value1", "field2", "value2"));

        SchemaAndValues sAndValues1 = SchemaAndValues.from(schema1, values);
        SchemaAndValues sAndValues2 = SchemaAndValues.from(schema2, values);
        SchemaAndValues sAndValues3 = SchemaAndValues.from(schema1, Collections.emptyMap());

        assertThat(sAndValues1.matches(sAndValues2)).isFalse();
        assertThat(sAndValues1.matches(sAndValues3)).isFalse();
    }

    @Test
    public void shouldNotMatchDueToDifferentNames() {
        Schema schema = Schema.from("schema", Set.of("key", "value"));
        SchemaAndValues schemaAndValues =
                SchemaAndValues.from(schema, Map.of("key", "aKey", "value", "aValue"));
        Schema otherSchema = Schema.from("anotherSchema", Set.of("key", "value"));
        SchemaAndValues otherSchemaAndValues =
                SchemaAndValues.from(otherSchema, Map.of("key", "aKey", "value", "aValue"));
        assertThat(schemaAndValues.matches(otherSchemaAndValues)).isFalse();
        assertThat(schemaAndValues.equals(otherSchemaAndValues)).isFalse();
    }

    @Test
    void shouldNotMatchDueToDifferentValues() {
        Schema schema = Schema.from("schema", Set.of("key", "value"));
        SchemaAndValues schemaAndValues =
                SchemaAndValues.from(schema, Map.of("key", "aKey", "value", "aValue"));
        Schema otherSchema = schema;
        SchemaAndValues anotherSchemaAndValues =
                SchemaAndValues.from(
                        otherSchema, Map.of("key", "anotherKey", "value", "aNotherValue"));
        assertThat(schemaAndValues.matches(anotherSchemaAndValues)).isFalse();
        assertThat(schemaAndValues.equals(anotherSchemaAndValues)).isFalse();
    }

    @Test
    void shouldCreateNopSchemaAndValues() {
        SchemaAndValues nop = SchemaAndValues.nop();

        assertThat(nop.values()).isEmpty();
        assertThat(nop.schema().keys()).isEmpty();
    }

    @Test
    void shouldMatchItselfAsNop() {
        SchemaAndValues nop = SchemaAndValues.nop();
        assertThat(nop.matches(nop)).isTrue();
    }

    @Test
    void shouldReturnAsText() {
        Schema schema = Schema.from("schema", Set.of("field1", "field2"));
        Map<String, String> values = new HashMap<>();
        values.put("BField", "value1");
        values.put("AField", "value2");
        SchemaAndValues schemaAndValues = SchemaAndValues.from(schema, values);

        // The order of keys in the output is granted to be alphabetical
        assertThat(schemaAndValues.asText()).isEqualTo("schema-[AField=value2,BField=value1]");
    }

    @Test
    void shouldReturnAsTextEmptyValues() {
        Schema schema = Schema.from("schema", Set.of("field1", "field2"));
        Map<String, String> values = Collections.emptyMap();
        SchemaAndValues schemaAndValues = SchemaAndValues.from(schema, values);

        assertThat(schemaAndValues.asText()).isEqualTo("schema-[]");
    }

    @Test
    void shouldReturnAsTextEmptyValuesWithEmptyKeys() {
        Schema schema = Schema.from("schema", Collections.emptySet());
        Map<String, String> values = Collections.emptyMap();
        SchemaAndValues schemaAndValues = SchemaAndValues.from(schema, values);

        assertThat(schemaAndValues.asText()).isEqualTo("schema");
    }

    @Test
    void shouldGenerateCorrectToStringWithNOP() {
        SchemaAndValues nop = SchemaAndValues.nop();
        assertThat(nop.asText()).isEqualTo("NOSCHEMA");
    }
}
