
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

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.lightstreamer.kafka.mapping.selectors.SelectorSupplier.Constant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class ParsersTest {

    static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of("name", "VALUE", Constant.VALUE),
                Arguments.of("name", "MYVALUE", Constant.VALUE));
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void shouldCreateParsingContext(String name, String expression, Constant expectedRoot) {
        Parsers.ParsingContext p = new Parsers.ParsingContext(name, expression, expectedRoot);
        assertThat(p.name()).isEqualTo(name);
        assertThat(p.expression()).isEqualTo(expression);
        assertThat(p.expectedRoot()).isEqualTo(expectedRoot);
    }

    @Test
    void shouldMatchRoot() {
        Parsers.ParsingContext p = new Parsers.ParsingContext("name", "VALUE", Constant.VALUE);
        assertDoesNotThrow(() -> p.matchRoot());

        Parsers.ParsingContext p2 = new Parsers.ParsingContext("name", "VALUE.", Constant.VALUE);
        p2.matchRoot();
    }

    @Test
    void shouldNotMatchRoot() {
        Parsers.ParsingContext p = new Parsers.ParsingContext("name", "MYVALUE", Constant.VALUE);
        ExpressionException ee = assertThrows(ExpressionException.class, () -> p.matchRoot());
        assertThat(ee.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [name]");
    }

    @Test
    void shouldNotMatchRoot2() {
        Parsers.ParsingContext p = new Parsers.ParsingContext("name", ".", Constant.VALUE);
        ExpressionException ee = assertThrows(ExpressionException.class, () -> p.matchRoot());
        assertThat(ee.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [name]");
    }

    @Test
    void shouldOneTokenFollowingRoot() {
        Parsers.ParsingContext p = new Parsers.ParsingContext("name", "VALUE.a", Constant.VALUE);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isFalse();
    }

    @Test
    void shouldMoreTokensFollowingRoot() {
        Parsers.ParsingContext p = new Parsers.ParsingContext("name", "VALUE.a.b", Constant.VALUE);
        p.matchRoot();
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("a");
        assertThat(p.hasNext()).isTrue();
        assertThat(p.next()).isEqualTo("b");
        assertThat(p.hasNext()).isFalse();
    }
}
