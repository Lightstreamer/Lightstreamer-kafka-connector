
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.test_utils.Records.record;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

public class ConstantSelectorTest {

    static ConstantSelector selector(ExtractionExpression expression) throws ExtractionException {
        return new ConstantSelectorSupplier().newSelector(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE.a", "KEY.b", "OFFSET.a"})
    void shouldNotCreateSelectorDueToNotAllowedAttributes(String expressionStr) {
        ExtractionExpression expression = Expression(expressionStr);
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> selector(expression));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Found the invalid expression [" + expressionStr + "] for scalar values");
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
        ExtractionExpression ee = Expression(expression);
        Data data = selector(ee).extract("field_name", record("record-key", "record-value"));
        assertThat(data.name()).isEqualTo("field_name");
        assertThat(data.text()).isEqualTo(expectedValue);
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
        ExtractionExpression expression = Expression(expressionStr);
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> {
                            new ConstantSelectorSupplier(Constant.KEY, Constant.VALUE)
                                    .newSelector(expression);
                        });
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Expected the root token [KEY|VALUE] while evaluating ["
                                + expressionStr
                                + "]");
    }

    @Test
    public void shouldCreateKeySelectorAndExtractData() throws ExtractionException {
        ExtractionExpression expression = Expression("KEY");
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.KEY);

        ConstantSelector selector = cs.newSelector(expression);
        Data data = selector.extractKey("field_name", record("record-key", "record-value"));
        assertThat(data.name()).isEqualTo("field_name");
        assertThat(data.text()).isEqualTo("record-key");
    }

    @Test
    public void shouldCreateValueSelectorAndExtractData() throws ExtractionException {
        ExtractionExpression expression = Expression("VALUE");
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.VALUE);

        ConstantSelector selector = cs.newSelector(expression);
        Data data = selector.extractValue("field_name", record("record-key", "record-value"));
        assertThat(data.name()).isEqualTo("field_name");
        assertThat(data.text()).isEqualTo("record-value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldNotCreateKeySelector(String expressionStr) {
        ExtractionExpression expression = Expression(expressionStr);
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.KEY);

        ExtractionException ee1 =
                assertThrows(
                        ExtractionException.class,
                        () -> {
                            cs.newSelector(expression);
                        });
        assertThat(ee1.getMessage())
                .isEqualTo(
                        "Expected the root token [KEY] while evaluating [" + expressionStr + "]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldNotCreateValueSelector(String expressionStr) {
        ExtractionExpression expression = Expression(expressionStr);
        ConstantSelectorSupplier cs = new ConstantSelectorSupplier(Constant.VALUE);

        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> {
                            cs.newSelector(expression);
                        });
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Expected the root token [VALUE] while evaluating [" + expressionStr + "]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldCreateEqualSelectors(String expressionStr) throws ExtractionException {
        ConstantSelector selector1 = selector(Expression(expressionStr));
        assertThat(selector1.equals(selector1)).isTrue();

        ConstantSelector selector2 = selector(Expression(expressionStr));
        assertThat(selector1.hashCode()).isEqualTo(selector2.hashCode());
        assertThat(selector1.equals(selector2)).isTrue();
    }

    @Test
    public void shouldCreateEqualKeySelectors() throws ExtractionException {
        KeySelector<Object> keySelector1 =
                ConstantSelectorSupplier.KeySelector().newSelector(Expression("KEY"));
        assertThat(keySelector1.equals(keySelector1)).isTrue();

        KeySelector<Object> keySelector2 =
                ConstantSelectorSupplier.KeySelector().newSelector(Expression("KEY"));
        assertThat(keySelector1.hashCode()).isEqualTo(keySelector2.hashCode());
        assertThat(keySelector1.equals(keySelector2)).isTrue();
    }

    @Test
    public void shouldCreateEqualValueSelectors() throws ExtractionException {
        KeySelector<Object> valueSelector1 =
                ConstantSelectorSupplier.ValueSelector().newSelector(Expression("VALUE"));
        assertThat(valueSelector1.equals(valueSelector1)).isTrue();

        KeySelector<Object> valueSelector2 =
                ConstantSelectorSupplier.ValueSelector().newSelector(Expression("VALUE"));
        assertThat(valueSelector1.hashCode()).isEqualTo(valueSelector2.hashCode());
        assertThat(valueSelector1.equals(valueSelector2)).isTrue();
    }
}
