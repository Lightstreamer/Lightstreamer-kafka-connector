
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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public class Split {

    public static record Pair(String key, String value) {}

    private static final String TEMPLATE_PATTERN = "\\s*%s\\s*";

    enum Separator {
        COMMA(','),
        COLON(':'),
        SEMICOLON(';'),
        EQUAL('=');

        private final char separator;

        Separator(char separator) {
            this.separator = separator;
        }

        public char separator() {
            return separator;
        }
    }

    private static final Pattern COMMA_WITH_WHITESPACE = splitPattern(Separator.COMMA.separator());
    private static final Pattern COLON_WITH_WHITESPACE = splitPattern(Separator.COLON.separator());
    private static final Pattern SEMICOLON_WITH_WHITESPACE =
            splitPattern(Separator.SEMICOLON.separator());
    private static final Pattern EQUAL_WITH_WHITESPACE = splitPattern(Separator.EQUAL.separator());

    public static Optional<Pair> asPairWithColon(String splittable) {
        return asPairInternal(byColon(splittable), false);
    }

    public static Optional<Pair> asPairWithColon(String splittable, boolean includeBlankValue) {
        return asPairInternal(byColon(splittable), includeBlankValue);
    }

    public static Optional<Pair> asPairWithEqual(String splittable) {
        return asPairInternal(byEqual(splittable), false);
    }

    public static Optional<Pair> asPairWithEqual(String splittable, boolean includeBlankValue) {
        return asPairInternal(byEqual(splittable), includeBlankValue);
    }

    public static Optional<Pair> asPair(
            String splittable, char separator, boolean includeBlankValue) {
        return asPairInternal(bySeparator(separator, splittable), includeBlankValue);
    }

    private static Optional<Pair> asPairInternal(List<String> tokens, boolean includeBlankValue) {
        int size = tokens.size();
        if (size > 2) {
            return Optional.empty();
        }
        String key = tokens.get(0);
        String value = size == 2 ? tokens.get(1) : "";
        if (key.isBlank() || (value.isBlank() && !includeBlankValue)) {
            return Optional.empty();
        }
        return Optional.of(new Pair(key, value));
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

    public static List<String> byEqual(String input) {
        return by(EQUAL_WITH_WHITESPACE, input);
    }

    public static List<String> bySeparator(char separator, String input) {
        return by(splitPattern(separator), input);
    }

    private static Pattern splitPattern(char separator) {
        return Pattern.compile(
                TEMPLATE_PATTERN.formatted(Pattern.quote(String.valueOf(separator))));
    }

    private static List<String> by(Pattern pattern, String input) {
        String trimmed = Objects.toString(input, "").trim();
        return Arrays.asList(pattern.split(trimmed, -1));
    }

    private Split() {}
}
