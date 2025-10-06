
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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public interface SchemaAndValues {

    Schema schema();

    Map<String, String> values();

    default boolean matches(SchemaAndValues other) {
        return schema().matches(other.schema()) && values().equals(other.values());
    }

    static SchemaAndValues from(Schema schema, Map<String, String> values) {
        return new DefaultSchemaAndValues(schema, values);
    }

    static SchemaAndValues from(String schemaName, Map<String, String> values) {
        Schema schema = Schema.from(schemaName, values.keySet());
        return new DefaultSchemaAndValues(schema, values);
    }

    static SchemaAndValues nop() {
        return DefaultSchemaAndValues.NOP;
    }

    static String format(SchemaAndValues sv) {
        Map<String, String> values = sv.values();
        if (values.isEmpty()) {
            return sv.schema().name();
        }

        return sv.schema().name()
                + "-"
                + values.entrySet().stream()
                        // .sorted(Map.Entry.comparingByKey())
                        .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(",", "[", "]"));
    }
}

class DefaultSchemaAndValues implements SchemaAndValues {

    static SchemaAndValues NOP = new DefaultSchemaAndValues(Schema.nop(), Collections.emptyMap());

    private final Schema schema;
    private final Map<String, String> values;

    DefaultSchemaAndValues(Schema schema, Map<String, String> values) {
        this.schema = schema;
        this.values = values;
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

    @Override
    public String toString() {
        return SchemaAndValues.format(this);
    }
}
