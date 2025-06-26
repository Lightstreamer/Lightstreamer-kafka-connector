
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

import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum Constant implements ExtractionExpression {
    KEY,
    VALUE,
    TIMESTAMP,
    PARTITION,
    OFFSET,
    TOPIC,
    HEADERS(true);

    private static final Map<String, Constant> NAME_CACHE;

    static {
        NAME_CACHE = Stream.of(values()).collect(toMap(Constant::toString, identity()));
    }

    public static final String VALUES_STR =
            Arrays.stream(Constant.values())
                    .map(a -> a.toString())
                    .collect(Collectors.joining("|"));

    public static Constant from(String name) {
        boolean indexed = false;
        if (name.endsWith("]") && name.contains("[")) {
            indexed = true;
            // Handle indexed constants like "KEY[0]"
            name = name.substring(0, name.indexOf('['));
        }
        Constant constant = NAME_CACHE.get(name);
        if (constant != null) {
            if (!constant.allowIndex() && indexed) {
                constant = null;
            }
        }
        return constant;
    }

    static Set<Constant> all() {
        return Arrays.stream(values()).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private final String tokens[] = new String[1];
    private final boolean allowIndex;

    Constant(boolean allowIndex) {
        this.allowIndex = allowIndex;
        Arrays.fill(tokens, toString());
    }

    Constant() {
        this(false);
    }

    @Override
    public Constant constant() {
        return this;
    }

    @Override
    public String[] tokens() {
        return tokens;
    }

    @Override
    public String expression() {
        return toString();
    }

    public boolean allowIndex() {
        return allowIndex;
    }
}
