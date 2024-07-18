
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

package com.lightstreamer.kafka.common.utils;

import com.google.re2j.Pattern;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Split {

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");
    private static final Pattern COLON_WITH_WHITESPACE = Pattern.compile("\\s*:\\s*");
    private static final Pattern SEMICOLON_WITH_WHITESPACE = Pattern.compile("\\s*;\\s*");

    public static record Pair(String key, String value) {}

    private Split() {}

    public static Optional<Pair> pair(String splittable) {
        List<String> tokens = byColon(splittable);
        if (tokens.size() == 2) {
            String key = tokens.get(0);
            String value = tokens.get(1);
            if (!(key.isBlank() || value.isBlank())) {
                return Optional.of(new Pair(key, value));
            }
        }
        return Optional.empty();
    }

    public static List<String> byComma(String input) {
        return by(COMMA_WITH_WHITESPACE, input);
    }

    public static List<String> byColon(String input) {
        return by(COLON_WITH_WHITESPACE, input);
    }

    public static List<String> bySemicolon(String input) {
        return by(SEMICOLON_WITH_WHITESPACE, input);
    }

    private static List<String> by(Pattern pattern, String input) {
        String trimmed = Objects.toString(input, "").trim();
        return Arrays.asList(pattern.split(trimmed, -1));
    }
}
