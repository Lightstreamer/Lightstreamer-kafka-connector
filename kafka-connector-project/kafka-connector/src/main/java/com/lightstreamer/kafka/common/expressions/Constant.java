
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

package com.lightstreamer.kafka.common.expressions;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum Constant {
    KEY,
    VALUE,
    TIMESTAMP,
    PARTITION,
    OFFSET,
    TOPIC;

    private static final Map<String, Constant> NAME_CACHE;

    static {
        NAME_CACHE = Stream.of(values()).collect(toMap(Constant::toString, identity()));
    }

    public static final String VALUES_STR =
            Arrays.stream(Constant.values())
                    .map(a -> a.toString())
                    .collect(Collectors.joining("|"));

    public static Constant from(String name) {
        return NAME_CACHE.get(name);
    }

    static Set<Constant> all() {
        return Arrays.stream(values()).collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
