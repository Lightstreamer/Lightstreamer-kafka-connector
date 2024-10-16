
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

public interface SchemaAndValues {

    Schema schema();

    Map<String, String> values();

    default boolean matches(SchemaAndValues other) {
        return schema().matches(other.schema()) && values().equals(other.values());
    }
}

class DefaultSchemaAndValues implements SchemaAndValues {

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
}
