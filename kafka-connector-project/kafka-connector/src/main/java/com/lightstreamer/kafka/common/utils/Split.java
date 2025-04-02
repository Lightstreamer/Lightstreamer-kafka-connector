
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

/**
 * Utility class for splitting strings into pairs or lists based on specific delimiters.
 *
 * <p>The {@code Split} class provides methods and an interface for splitting strings into key-value
 * pairs or lists of substrings. It supports various delimiters such as commas, colons, and
 * semicolons, and allows for custom delimiters through the {@code Splitter} interface.
 *
 * <p>Key features include:
 *
 * <ul>
 *   <li>Splitting strings into key-value pairs with optional blank value handling.
 *   <li>Splitting strings into lists of substrings based on predefined or custom delimiters.
 *   <li>Support for whitespace trimming around delimiters.
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * List<String> list = Split.byComma("a, b, c");
 * Optional<Split.Pair> pair = Split.asPairWithColon("key:value");
 * Split.Splitter splitter = Split.on(';');
 * List<String> customList = splitter.splitToList("x;y;z");
 * }</pre>
 */
public class Split {

    /**
     * Defines operations for splitting strings into pairs or lists based on specific delimiters.
     *
     * <p>This interface provides methods to split input strings either into key-value pairs or into
     * lists of strings according to implementation-specific splitting rules.
     */
    public interface Splitter {

        /**
         * Splits the input string into a key-value pair using the default splitting strategy. This
         * is a convenience method that delegates to {@link #splitToPair(String, boolean)} with
         * relaxed validation (false).
         *
         * @param input the string to be split into a pair
         * @return an {@code Optional} containing the resulting {@code Pair} if the split operation
         *     was successful, or an empty {@code Optional} if the input could not be split
         */
        default Optional<Pair> splitToPair(String input) {
            return splitToPair(input, false);
        }

        /**
         * Splits a given input string into a pair based on a defined delimiter.
         *
         * @param input the string to be split
         * @param includeBlankValue if {@code true}, allows blank/empty values in the resulting
         *     pair; if {@code false}, returns empty {@code Optional} for inputs containing blank
         *     values
         * @return an {@code Optional} containing a {@code Pair} with the split values if
         *     successful, or an empty {@code Optional} if the split operation cannot be performed
         *     or blank values are not allowed (based on {@code includeBlankValue} parameter)
         */
        Optional<Pair> splitToPair(String input, boolean includeBlankValue);

        /**
         * Splits a string into a list of substrings.
         *
         * @param input the string to split
         * @return a list containing the substrings resulting from splitting the input string
         */
        List<String> splitToList(String input);
    }

    private static class SplitterImpl implements Splitter {
        private final Pattern pattern;

        SplitterImpl(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public Optional<Pair> splitToPair(String input, boolean includeBlankValue) {
            return Split.asPairInternal(Split.by(pattern, input), includeBlankValue);
        }

        @Override
        public List<String> splitToList(String input) {
            return Split.by(pattern, input);
        }
    }

    /**
     * A record representing a pair of key and value. This is a simple immutable data structure that
     * holds two related pieces of data.
     *
     * @param key The key part of the pair.
     * @param value The value part of the pair.
     */
    public static record Pair(String key, String value) {}

    private static final String TEMPLATE_PATTERN = "\\s*%s\\s*";

    private enum Separator {
        COLON(':'),
        COMMA(','),
        SEMICOLON(';');

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

    public static List<String> byComma(String input) {
        return by(COMMA_WITH_WHITESPACE, input);
    }

    public static List<String> byColon(String input) {
        return by(COLON_WITH_WHITESPACE, input);
    }

    public static List<String> bySemicolon(String input) {
        return by(SEMICOLON_WITH_WHITESPACE, input);
    }

    public static List<String> bySeparator(char separator, String input) {
        return by(splitPattern(separator), input);
    }

    public static Optional<Pair> asPairWithColon(String splittable) {
        return asPairInternal(byColon(splittable), false);
    }

    public static Splitter on(char separator) {
        return new SplitterImpl(splitPattern(separator));
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
