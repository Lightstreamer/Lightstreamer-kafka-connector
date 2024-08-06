
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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.utils.Split.Pair;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

public class SplitTest {

    static Stream<Arguments> semicolonTestValues() {
        return values(';');
    }

    static Stream<Arguments> colonTestValues() {
        return values(':');
    }

    static Stream<Arguments> commaTestValues() {
        return values(',');
    }

    static Stream<Arguments> testValues() {
        return values('@');
    }

    static Stream<Arguments> values(char symbol) {
        return Stream.of(
                arguments(null, List.of("")),
                arguments("", List.of("")),
                arguments("  ", List.of("")),
                arguments("%s".formatted(symbol), List.of("", "")),
                arguments("a%s".formatted(symbol), List.of("a", "")),
                arguments(" a%s ".formatted(symbol), List.of("a", "")),
                arguments("a%sb".formatted(symbol), List.of("a", "b")),
                arguments("a%sb  ".formatted(symbol), List.of("a", "b")),
                arguments("%sb".formatted(symbol), List.of("", "b")),
                arguments("  %sb  ".formatted(symbol), List.of("", "b")));
    }

    @ParameterizedTest
    @MethodSource("semicolonTestValues")
    void shouldSplitBySemicolon(String input, List<String> expected) {
        assertThat(Split.bySemicolon(input)).containsExactlyElementsIn(expected);
    }

    @ParameterizedTest
    @MethodSource("colonTestValues")
    void shouldSplitByColon(String input, List<String> expected) {
        assertThat(Split.byColon(input)).containsExactlyElementsIn(expected);
    }

    @ParameterizedTest
    @MethodSource("commaTestValues")
    void shouldSplitByComma(String input, List<String> expected) {
        assertThat(Split.byComma(input)).containsExactlyElementsIn(expected);
    }

    @ParameterizedTest
    @MethodSource("testValues")
    void shouldSplitByAnySeparator(String input, List<String> expected) {
        assertThat(Split.bySeparator('@', input)).containsExactlyElementsIn(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"a:b", "  a:b  ", "a:  b ", "  a:b", " a  : b  "})
    void shouldReturnPair(String splittable) {
        assertThat(Split.asPair(splittable)).hasValue(new Pair("a", "b"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"a", "a:", "  :b  ", ":  b ", ":", " : "})
    void shouldReturnEmptyPair(String splittable) {
        assertThat(Split.asPair(splittable)).isEmpty();
    }
}
