
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

package com.lightstreamer.kafka.mapping.selectors;

import java.util.Collections;
import java.util.Set;

public interface Schema {

    Set<String> keys();

    String name();

    default boolean isEmpty() {
        return keys().isEmpty();
    }

    public default boolean matches(Schema other) {
        return this.equals(other);
    }

    static Schema from(String name, Set<String> keys) {
        return new DefaultSchema(name, keys);
    }

    static Schema empty(String name) {
        return new DefaultSchema(name, Collections.emptySet());
    }
}

record DefaultSchema(String name, Set<String> keys) implements Schema {}
