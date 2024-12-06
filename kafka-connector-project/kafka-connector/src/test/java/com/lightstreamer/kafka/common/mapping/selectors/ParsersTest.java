
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class ParsersTest {

    static Stream<Arguments> args() {
        return Stream.of(
                arguments("name", Expressions.Expression("VALUE"), Constant.VALUE),
                arguments("name", Expressions.Expression("KEY.attrib"), Constant.KEY));
    }

    @ParameterizedTest
    @MethodSource("args")
    void shouldCreateParsingContext(
            String name, ExtractionExpression expression, Constant expectedRoot) {
        Parsers.ParsingContext p1 = new Parsers.ParsingContext(name, expression, expectedRoot);
        assertThat(p1.name()).isEqualTo(name);
        assertThat(p1.expression()).isEqualTo(expression.toString());
        assertThat(p1.expectedRoot()).isEqualTo(expectedRoot);
        assertThat(p1.checkStructured()).isTrue();

        p1 = new Parsers.ParsingContext(name, expression, expectedRoot, false);
        assertThat(p1.name()).isEqualTo(name);
        assertThat(p1.expression()).isEqualTo(expression.toString());
        assertThat(p1.expectedRoot()).isEqualTo(expectedRoot);
        assertThat(p1.checkStructured()).isFalse();
    }

    @Test
    void shouldMatchRoot() throws ExtractionException {
        Parsers.ParsingContext p =
                new Parsers.ParsingContext("name", Expressions.Expression("VALUE"), Constant.VALUE);
        assertDoesNotThrow(() -> p.matchRoot());
    }

    @Test
    void shouldNotMatchRoot() {
        Parsers.ParsingContext p =
                new Parsers.ParsingContext(
                        "name", Expressions.Expression("KEY.attrib"), Constant.VALUE);
        ExtractionException ee = assertThrows(ExtractionException.class, () -> p.matchRoot());
        assertThat(ee.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [name]");
    }

    @Test
    void shouldOneTokenFollowingRoot() throws ExtractionException {
        Parsers.ParsingContext p =
                new Parsers.ParsingContext(
                        "name", Expressions.Expression("VALUE.a"), Constant.VALUE);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isFalse();
    }

    @Test
    void shouldMoreTokensFollowingRoot() throws ExtractionException {
        Parsers.ParsingContext p =
                new Parsers.ParsingContext(
                        "name", Expressions.Expression("VALUE.a.b"), Constant.VALUE);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("b");
        assertThat(p.hasNext()).isFalse();
    }
}
