
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
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.record;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

public class ConstantSelectorTest {

    static ConstantSelector selector(ExtractionExpression expression) throws ExtractionException {
        return new ConstantSelectorSupplier().newSelector("field_name", expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE.a", "KEY.b", "OFFSET.a"})
    void shouldNotCreateSelectorDueToNotAllowedAttributes(String expressionStr) {
        ExtractionExpression expression = Expressions.expression(expressionStr);
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> selector(expression));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Found the invalid expression ["
                                + expressionStr
                                + "] for scalar values while evaluating [field_name]");
    }

    @ParameterizedTest
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
    public void shouldExtractValue(String expression, String expectedValue)
            throws ExtractionException {
        ExtractionExpression ee = Expressions.expression(expression);
        Data value = selector(ee).extract(record("record-key", "record-value"));
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo(expectedValue);
    }

    static Stream<Arguments> args() {
        return Stream.of(
                arguments("KEY", new Constant[] {Constant.KEY}),
                arguments("KEY|VALUE", new Constant[] {Constant.KEY, Constant.VALUE}),
                arguments(
                        "PARTITION|OFFSET", new Constant[] {Constant.PARTITION, Constant.OFFSET}));
    }

    @ParameterizedTest
    @MethodSource("args")
    public void shouldDumpExpectedConstantString(String expectedString, Constant... constant) {
        assertThat(new ConstantSelectorSupplier(constant).expectedConstantStr())
                .isEqualTo(expectedString);
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldNotCreateDueToNotAllowedConstant(String expressionStr) {
        ExtractionExpression expression = Expressions.expression(expressionStr);
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> {
                            new ConstantSelectorSupplier(Constant.KEY, Constant.VALUE)
                                    .newSelector("field_name", expression);
                        });
        assertThat(ee.getMessage())
                .isEqualTo("Expected the root token [KEY|VALUE] while evaluating [field_name]");
    }

    @Test
    public void shouldCreateKeySelectorAndExtractData() throws ExtractionException {
        ExtractionExpression expression = Expressions.expression("KEY");
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.KEY);

        KeySelector<Object> newKeySelector = cs.newKeySelector("field_name", expression);
        Data value = newKeySelector.extractKey(record("record-key", "record-value"));
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo("record-key");
    }

    @Test
    public void shouldCreateValueSelectorAndExtractData() throws ExtractionException {
        ExtractionExpression expression = Expressions.expression("VALUE");
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.VALUE);

        KeySelector<Object> newKeySelector = cs.newKeySelector("field_name", expression);
        Data value = newKeySelector.extractKey(record("record-key", "record-value"));
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo("record-value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldNotCreateKeySelector(String expressionStr) {
        ExtractionExpression expression = Expressions.expression(expressionStr);
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.KEY);

        ExtractionException ee1 =
                assertThrows(
                        ExtractionException.class,
                        () -> {
                            cs.newKeySelector("field_name", expression);
                        });
        assertThat(ee1.getMessage())
                .isEqualTo("Expected the root token [KEY] while evaluating [field_name]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldNotCreatValueSelector(String expressionStr) {
        ExtractionExpression expression = Expressions.expression(expressionStr);
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.VALUE);

        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> {
                            cs.newValueSelector("field_name", expression);
                        });
        assertThat(ee.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [field_name]");
    }
}
