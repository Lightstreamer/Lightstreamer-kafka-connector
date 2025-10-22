
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public interface SchemaAndValues {

    Schema schema();

    Map<String, String> values();

    default String asText() {
        if (schema().keys().isEmpty()) {
            return schema().name();
        }

        String suffix =
                new TreeMap<>(values())
                        .entrySet().stream()
                                .map(entry -> entry.getKey() + "=" + entry.getValue())
                                .collect(Collectors.joining(","));

        return String.format("%s-[%s]", schema().name(), suffix);
    }

    default boolean matches(SchemaAndValues other) {
        return schema().matches(other.schema()) && values().equals(other.values());
    }

    static SchemaAndValues from(String schemaName, Map<String, String> values) {
        Schema schema = Schema.from(schemaName, values.keySet());
        return from(schema, values);
    }

    static SchemaAndValues from(Schema schema, Map<String, String> values) {
        return new DefaultSchemaAndValues(schema, values);
    }

    static SchemaAndValues nop() {
        return DefaultSchemaAndValues.NOP;
    }
}

class BuildableSchemaAndValues implements SchemaAndValues {

    private final Schema schema;
    private final Map<String, String> values;

    public BuildableSchemaAndValues(Schema schema) {
        this.schema = schema;
        this.values = new HashMap<>();
    }

    public BuildableSchemaAndValues addValue(String key, String value) {
        if (schema.keys().contains(key)) {
            values.put(key, value);
        }
        return this;
    }

    public BuildableSchemaAndValues addValueNoKeyCheck(String key, String value) {
        values.put(key, value);
        return this;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Map<String, String> values() {
        return values;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;

        return obj instanceof SchemaAndValues other && this.matches(other);
    }

    public int hashCode() {
        return Objects.hash(schema, values);
    }
}

class DefaultSchemaAndValues implements SchemaAndValues {

    /** A constant representing a no-operation (NOP) schema with no values. */
    static final SchemaAndValues NOP =
            new DefaultSchemaAndValues(Schema.nop(), Collections.emptyMap());

    private final Schema schema;
    private final Map<String, String> values;

    DefaultSchemaAndValues(Schema schema, Map<String, String> values) {
        this.schema = schema;
        this.values = Collections.unmodifiableMap(values);
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Map<String, String> values() {
        return values;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;

        return obj instanceof SchemaAndValues other && this.matches(other);
    }

    public int hashCode() {
        return Objects.hash(schema, values);
    }

    public String toString() {
        return asText();
    }
}
