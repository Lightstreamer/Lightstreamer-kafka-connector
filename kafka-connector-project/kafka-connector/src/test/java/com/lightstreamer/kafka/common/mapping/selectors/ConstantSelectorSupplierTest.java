
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.OFFSET;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.PARTITION;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.TIMESTAMP;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.TOPIC;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecord;

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

import java.util.List;
import java.util.stream.Stream;

public class ConstantSelectorSupplierTest {

    static GenericSelector selector(String expression) throws ExtractionException {
        return ConstantSelectorSupplier.makeSelectorSupplier(
                        VALUE, KEY, OFFSET, TIMESTAMP, PARTITION, TOPIC)
                .newSelector(Wrapped("#{" + expression + "}"));
    }

    static Stream<Arguments> constants() {
        return Stream.of(
                arguments(List.of(KEY), "KEY"),
                arguments(List.of(KEY, VALUE), "KEY|VALUE"),
                arguments(List.of(OFFSET, TIMESTAMP, VALUE), "OFFSET|TIMESTAMP|VALUE"),
                arguments(List.of(TOPIC, TIMESTAMP, VALUE), "TOPIC|TIMESTAMP|VALUE"),
                arguments(List.of(PARTITION, OFFSET), "PARTITION|OFFSET"));
    }

    @ParameterizedTest
    @MethodSource("constants")
    public void shouldMakeConstantSelectorSupplier(
            List<Constant> constants, String expectedString) {
        ConstantSelectorSupplier constantSelectorSupplier =
                ConstantSelectorSupplier.makeSelectorSupplier(constants.toArray(new Constant[0]));
        assertThat(constantSelectorSupplier.expectedConstantStr()).isEqualTo(expectedString);
    }

    @Test
    public void shouldNotMakeConstantSelectorSupplier() {
        IllegalArgumentException iae =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> ConstantSelectorSupplier.makeSelectorSupplier(HEADERS, VALUE));
        assertThat(iae).hasMessageThat().isEqualTo("Cannot handle HEADERS constant");
    }

    @ParameterizedTest
    @MethodSource("constants")
    public void shouldMakeConstantSelector(List<Constant> constants, String expectedString)
            throws ExtractionException {
        ConstantSelectorSupplier constantSelectorSupplier =
                ConstantSelectorSupplier.makeSelectorSupplier(constants.toArray(new Constant[0]));
        for (Constant constant : constants) {
            ExtractionExpression expression = Wrapped("#{" + constant.name() + "}");
            GenericSelector selector = constantSelectorSupplier.newSelector(expression);
            assertThat(selector.expression().expression()).isEqualTo(expression.expression());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldNotMakeConstantSelectorSupplierDueToUnexpectedRootToken(String expression) {
        ConstantSelectorSupplier s = ConstantSelectorSupplier.makeSelectorSupplier(KEY, VALUE);
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> s.newSelector(Wrapped("#{" + expression + "}")));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo(
                        "Expected the root token [KEY|VALUE] while evaluating ["
                                + expression
                                + "]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE.a", "KEY.b", "OFFSET.a"})
    public void shouldNotMakeConstantSelectorDueToNotAllowedAttributes(String expression) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> selector(expression));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo("Found the invalid expression [" + expression + "] for scalar values");
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION | EXPECTED_VALUE
                KEY        | record-key
                VALUE      | record-value
                TIMESTAMP  | -1
                PARTITION  | 150
                OFFSET     | 120
                TOPIC      | record-topic
                    """)
    public void shouldExtractData(String expression, String expectedValue)
            throws ExtractionException {
        GenericSelector selector = selector(expression);

        Data autoBoundData =
                selector.extract("field_name", KafkaRecord("record-key", "record-value"));
        assertThat(autoBoundData.name()).isEqualTo("field_name");
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData = selector.extract(KafkaRecord("record-key", "record-value"));
        assertThat(boundData.name()).isEqualTo(expression);
        assertThat(boundData.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldExtractNullData() throws ExtractionException {
        GenericSelector keySelector = selector("KEY");

        Data nullKey = keySelector.extract(KafkaRecord(null, "record-value"));
        assertThat(nullKey.name()).isEqualTo("KEY");
        assertThat(nullKey.text()).isNull();

        GenericSelector valueSelector = selector("VALUE");
        Data nullValue = valueSelector.extract(KafkaRecord("record-key", null));
        assertThat(nullValue.name()).isEqualTo("VALUE");
        assertThat(nullValue.text()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"TOPIC", "PARTITION", "OFFSET", "TIMESTAMP"})
    public void shouldCreateEqualSelectors(String expression) throws ExtractionException {
        GenericSelector selector1 = selector(expression);
        assertThat(selector1.equals(selector1)).isTrue();

        GenericSelector selector2 = selector(expression);
        assertThat(selector1.hashCode()).isEqualTo(selector2.hashCode());
        assertThat(selector1.equals(selector2)).isTrue();
    }
}
