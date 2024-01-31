/*
*
* Copyright (c) Lightstreamer Srl
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public interface Schema {

    record MatchResult(Set<String> matchedKeys, boolean matched) {
    }

    Set<String> keys();

    String name();

    default boolean isEmpty() {
        return keys().isEmpty();
    }

    default public MatchResult matches(Schema other) {
        if (!name().equals(other.name())) {
            return new MatchResult(Collections.emptySet(), false);
        }
        Set<String> thisKeys = keys();
        Set<String> otherKeys = other.keys();

        HashSet<String> matchedKeys = new HashSet<>(thisKeys);
        matchedKeys.retainAll(otherKeys);

        return new MatchResult(matchedKeys, thisKeys.containsAll(otherKeys));
    }

    static Schema from(String name, Set<String> keys) {
        return new DefaultSchema(name, keys);
    }

    static Schema empty(String name) {
        return new DefaultSchema(name, Collections.emptySet());
    }

}

record DefaultSchema(String name, Set<String> keys) implements Schema {
}
