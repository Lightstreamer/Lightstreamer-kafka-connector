
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

package com.lightstreamer.kafka.adapters.mapping.selectors.kvp;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.KVP;
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

public class KvpNodeSelectorsSuppliersTest {

    // A configuration with proper evaluator type settings for key and value
    static ConnectorConfig CONFIG =
            ConnectorConfigProvider.minimalWith(
                    Map.of(
                            RECORD_KEY_EVALUATOR_TYPE,
                            KVP.toString(),
                            RECORD_VALUE_EVALUATOR_TYPE,
                            KVP.toString()));
    private String INPUT =
            "QCHARTTOT=2032,TRow=12790,QV=9,PV=43,TMSTMP=2024-04-3013:23:07,QCHART=1,VTOT=81316,QTOT=2032,O=30/04/2024-13:23:07,QA=9012,Q=1,PA=40,PCHART=43,NTRAD=106,NOVALUE,NOVALUE2=";

    static ValueSelector<String> valueSelector(ExtractionExpression expression)
            throws ExtractionException {
        return valueSelector(expression, CONFIG);
    }

    static ValueSelector<String> valueSelector(
            ExtractionExpression expression, ConnectorConfig config) throws ExtractionException {
        return new KvpSelectorsSuppliers(config)
                .makeValueSelectorSupplier()
                .newSelector("name", expression);
    }

    static KeySelector<String> keySelector(ExtractionExpression expression)
            throws ExtractionException {
        return keySelector(expression, CONFIG);
    }

    static KeySelector<String> keySelector(ExtractionExpression expression, ConnectorConfig config)
            throws ExtractionException {
        return new KvpSelectorsSuppliers(config)
                .makeKeySelectorSupplier()
                .newSelector("name", expression);
    }

    @Test
    public void shouldMakeKeySelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, KVP.toString()));
        KvpSelectorsSuppliers s = new KvpSelectorsSuppliers(config);
        KeySelectorSupplier<String> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.deserializer().getClass())
                .isEqualTo(StringDeserializer.class);
    }

    @Test
    public void shouldMakeValueSelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, KVP.toString()));
        KvpSelectorsSuppliers s = new KvpSelectorsSuppliers(config);
        ValueSelectorSupplier<String> keySelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(keySelectorSupplier.deserializer().getClass())
                .isEqualTo(StringDeserializer.class);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,      EXPECTED
                        VALUE.QCHARTTOT, 2032
                        VALUE.TRow,      12790
                        VALUE.QV,        9
                        VALUE.PV,        43
                        VALUE.TMSTMP,    2024-04-3013:23:07
                        VALUE.QCHART,    1
                        VALUE.VTOT,      81316
                        VALUE.QTOT,      2032
                        VALUE.O,         30/04/2024-13:23:07
                        VALUE.QA,        9012
                        VALUE.Q,         1
                        VALUE.PA,        40
                        VALUE.PCHART,    43
                        VALUE.NTRAD,     106
                        VALUE.NOVALUE,   <EMPTY>
                        VALUE.NOVALUE2,  <EMPTY>
                        """)
    public void shouldExtractValue(String expressionStr, String expected)
            throws ExtractionException {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        StringSubject subject =
                assertThat(valueSelector(expression).extractValue(fromValue(INPUT)).text());
        if (expected.equals("<EMPTY>")) {
            subject.isEmpty();
        } else {
            subject.isEqualTo(expected);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                EXPECTED_ERROR_MESSAGE
                        VALUE,                     The expression [VALUE] must evaluate to a non-complex object
                        VALUE.no_attrib,           Field [no_attrib] not found
                        VALUE.no_children[0],      Field [no_children] not found
                        VALUE.QCHARTTOT[0],        Field [QCHARTTOT] is not indexed
                        VALUE.QCHARTTOT['no_key'], Cannot retrieve field [no_key] from a scalar object
                        VALUE.QCHARTTOT.no_key,    Cannot retrieve field [no_key] from a scalar object
                        VALUE.NOVALUE.no_key,      Cannot retrieve field [no_key] from a scalar object
                        """)
    public void shouldNotExtractValue(String expressionStr, String errorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> valueSelector(expression).extractValue(fromValue(INPUT)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                        EXPRESSION | EXPECTED
                        VALUE      | {A=1, B=2}
                        VALUE.A    | 1
                        VALUE.B    | 2
                        """)
    public void shouldExtractValueWithNonDefaultSettings(String expressionString, String expected)
            throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                KVP.toString(),
                                RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR,
                                "@",
                                RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR,
                                "|"));

        String message = "A@1|B@2";
        ExtractionExpression expression = Expressions.Expression(expressionString);
        String text =
                valueSelector(expression, config).extractValue(fromValue(message), false).text();
        assertThat(text).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,    EXPECTED
                        KEY.QCHARTTOT, 2032
                        KEY.TRow,      12790
                        KEY.QV,        9
                        KEY.PV,        43
                        KEY.TMSTMP,    2024-04-3013:23:07
                        KEY.QCHART,    1
                        KEY.VTOT,      81316
                        KEY.QTOT,      2032
                        KEY.O,         30/04/2024-13:23:07
                        KEY.QA,        9012
                        KEY.Q,         1
                        KEY.PA,        40
                        KEY.PCHART,    43
                        KEY.NTRAD,     106
                        KEY.NOVALUE,   <EMPTY>
                        KEY.NOVALUE2,  <EMPTY>
                        """)
    public void shouldExtractKey(String expressionStr, String expected) throws ExtractionException {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        StringSubject subject =
                assertThat(keySelector(expression).extractKey(fromKey(INPUT)).text());
        if (expected.equals("<EMPTY>")) {
            subject.isEmpty();
        } else {
            subject.isEqualTo(expected);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,              EXPECTED_ERROR_MESSAGE
                        KEY.no_attrib,           Field [no_attrib] not found
                        KEY.no_children[0],      Field [no_children] not found
                        KEY.QCHARTTOT[0],        Field [QCHARTTOT] is not indexed
                        KEY.QCHARTTOT['no_key'], Cannot retrieve field [no_key] from a scalar object
                        KEY.QCHARTTOT.no_key,    Cannot retrieve field [no_key] from a scalar object
                        KEY.NOVALUE.no_key,      Cannot retrieve field [no_key] from a scalar object
                        """)
    public void shouldNotExtractKey(String expressionStr, String errorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> keySelector(expression).extractKey(fromKey(INPUT)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                        EXPRESSION | EXPECTED
                        KEY        | {A=1, B=2}
                        KEY.A      | 1
                        KEY.B      | 2
                        """)
    public void shouldExtractKeyWithNonDefaultSettings(String expressionString, String expected)
            throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                KVP.toString(),
                                RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR,
                                "@",
                                RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR,
                                "|"));

        String message = "A@1|B@2";
        ExtractionExpression expression = Expressions.Expression(expressionString);
        String text = keySelector(expression, config).extractKey(fromKey(message), false).text();
        assertThat(text).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,         EXPECTED_ERROR_MESSAGE
                        VALUE.a. .b,        Found the invalid expression [VALUE.a. .b] with missing tokens while evaluating [name]
                        VALUE.attrib[],     Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[0]xsd, Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
                        VALUE.attrib[],     Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[a],    Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
                    """)
    public void shouldNotCreateValueSelector(String expressionStr, String expectedErrorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,       EXPECTED_ERROR_MESSAGE
                        KEY.a. .b,        Found the invalid expression [KEY.a. .b] with missing tokens while evaluating [name]
                        KEY.attrib[],     Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[0]xsd, Found the invalid indexed expression [KEY.attrib[0]xsd] while evaluating [name]
                        KEY.attrib[],     Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[a],    Found the invalid indexed expression [KEY.attrib[a]] while evaluating [name]
                    """)
    public void shouldNotCreateKeySelector(String expressionStr, String expectedErrorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
