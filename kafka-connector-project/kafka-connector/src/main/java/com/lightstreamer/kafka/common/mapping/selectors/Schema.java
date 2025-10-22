
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

import static java.util.Collections.emptySet;

import java.util.Objects;
import java.util.Set;

public interface Schema {

    Set<String> keys();

    String name();

    static Schema from(String name, Set<String> keys) {
        return new DefaultSchema(name, keys);
    }

    static Schema empty(String name) {
        return from(name, emptySet());
    }

    static Schema nop() {
        return DefaultSchema.NOP;
    }
}

final class DefaultSchema implements Schema {

    static Schema NOP = Schema.empty("NOSCHEMA");

    private final String name;
    private final Set<String> keys;

    DefaultSchema(String name, Set<String> keys) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Schema name must be a non empty string");
        }
        this.name = name;
        this.keys = keys;
    }

    @Override
    public Set<String> keys() {
        return keys;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        return obj instanceof DefaultSchema other
                && Objects.equals(name, other.name)
                && keys.equals(other.keys);
    }

    public int hashCode() {
        int result = 17;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (keys != null ? keys.hashCode() : 0);
        return result;
    }

    public String toString() {
        return String.format("(%s-%s)", name(), keys());
    }
}
