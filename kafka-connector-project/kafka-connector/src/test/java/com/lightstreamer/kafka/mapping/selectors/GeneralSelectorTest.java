
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
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.record;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.mapping.selectors.SelectorSupplier.Constant;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

public class GeneralSelectorTest {

    static GeneralSelector selector(String expression) {
        return new GeneralSelectorSupplier().newSelector("field_name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,     VALUE
                        KEY,            record-key
                        VALUE,          record-value
                        TIMESTAMP,      -1
                        PARTITION,      150
                        OFFSET,         120
                        TOPIC,          record-topic
                    """)
    public void shouldExtractValue(String expression, String expectedValue) {
        Value value = selector(expression).extract(record("record-key", "record-value"));
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo(expectedValue);
    }

    static Stream<Arguments> args() {
        return Stream.of(
                Arguments.of("KEY", new Constant[] {Constant.KEY}),
                Arguments.of("KEY|VALUE", new Constant[] {Constant.KEY, Constant.VALUE}),
                Arguments.of(
                        "PARTITION|OFFSET", new Constant[] {Constant.PARTITION, Constant.OFFSET}));
    }

    @ParameterizedTest
    @MethodSource("args")
    public void shouldDumpExpectedConstantString(String expectedString, Constant... constant) {
        assertThat(new GeneralSelectorSupplier(constant).expectedConstantStr())
                .isEqualTo(expectedString);
    }

    @ParameterizedTest(name = "[{argumentsWithNames}]")
    @EmptySource
    @NullSource
    @ValueSource(strings = {"NOT-EXISTING-ATTRIBUTE", "PARTITION.", "..", "@", "\\"})
    public void shouldNotCreateSelector(String expression) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> selector(expression));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Expected the root token [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC] while"
                                + " evaluating [field_name]");
    }

    @ParameterizedTest(name = "[{argumentsWithNames}]")
    @EmptySource
    @NullSource
    @ValueSource(strings = {"NOT-EXISTING-ATTRIBUTE", "PARTITION.", "..", "@", "\\"})
    public void shouldNotCreate2(String expression) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class,
                        () ->
                                new GeneralSelectorSupplier(Constant.KEY, Constant.VALUE)
                                        .newSelector("field_name", expression));
        assertThat(ee.getMessage())
                .isEqualTo("Expected the root token [KEY|VALUE] while evaluating [field_name]");
    }
}
