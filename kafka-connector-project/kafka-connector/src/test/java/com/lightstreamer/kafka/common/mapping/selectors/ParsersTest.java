
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

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.HEADERS;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedWithWildcards;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class ParsersTest {

    static Stream<Arguments> args() {
        return Stream.of(
                arguments(Wrapped("#{VALUE}"), VALUE),
                arguments(Wrapped("#{KEY.attrib}"), KEY),
                arguments(Wrapped("#{HEADERS}"), HEADERS),
                arguments(Wrapped("#{HEADERS[1]}"), HEADERS),
                arguments(Wrapped("#{HEADERS['key']}"), HEADERS),
                arguments(Wrapped("#{VALUE}"), VALUE),
                arguments(Wrapped("#{KEY.attrib}"), KEY),
                arguments(Wrapped("#{HEADERS}"), HEADERS),
                arguments(Wrapped("#{HEADERS[1]}"), HEADERS),
                arguments(Wrapped("#{HEADERS['key']}"), HEADERS));
    }

    @ParameterizedTest
    @MethodSource("args")
    void shouldCreateParsingContext(ExtractionExpression expression, Constant expectedRoot) {
        Parsers.ParsingContext p = new Parsers.ParsingContext(expression, expectedRoot);
        assertThat(p.expression()).isEqualTo(expression.toString());
        assertThat(p.expectedRoot()).isEqualTo(expectedRoot);
    }

    @Test
    void shouldMatchRoot() throws ExtractionException {
        Parsers.ParsingContext p = new Parsers.ParsingContext(Wrapped("#{VALUE}"), VALUE);
        assertDoesNotThrow(() -> p.matchRoot());
    }

    @Test
    void shouldNotMatchRoot() {
        Parsers.ParsingContext p = new Parsers.ParsingContext(Wrapped("#{KEY.attrib}"), VALUE);
        ExtractionException ee = assertThrows(ExtractionException.class, () -> p.matchRoot());
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo("Expected the root token [VALUE] while evaluating [KEY.attrib]");
    }

    @Test
    void shouldHaveOneTokenFollowingTheRoot() throws ExtractionException {
        Parsers.ParsingContext p = new Parsers.ParsingContext(Wrapped("#{VALUE.a}"), VALUE);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("VALUE");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isFalse();
    }

    @Test
    void shouldWildCharTerminateTheExpression() throws ExtractionException {
        Parsers.ParsingContext p =
                new Parsers.ParsingContext(WrappedWithWildcards("#{VALUE.a.b.*}"), VALUE);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("VALUE");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("b");
    }

    @Test
    void shouldHaveMoreTokensFollowingTheRoot() throws ExtractionException {
        Parsers.ParsingContext p = new Parsers.ParsingContext(Wrapped("#{VALUE.a.b}"), VALUE);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("VALUE");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("b");
        assertThat(p.hasNext()).isFalse();
    }

    @Test
    void shouldHaveMoreTokensFollowingHeaders() throws ExtractionException {
        Parsers.ParsingContext p = new Parsers.ParsingContext(Wrapped("#{HEADERS.a}"), HEADERS);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("HEADERS");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isFalse();
    }
}
