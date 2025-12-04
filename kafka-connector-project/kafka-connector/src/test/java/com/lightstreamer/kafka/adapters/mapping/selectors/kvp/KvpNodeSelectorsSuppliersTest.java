
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
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

    static KeySelector<String> keySelector(ExtractionExpression expression)
            throws ExtractionException {
        return keySelector(expression, CONFIG);
    }

    static KeySelector<String> keySelector(ExtractionExpression expression, ConnectorConfig config)
            throws ExtractionException {
        return new KvpSelectorsSuppliers(config).makeKeySelectorSupplier().newSelector(expression);
    }

    static ValueSelector<String> valueSelector(ExtractionExpression expression)
            throws ExtractionException {
        return valueSelector(expression, CONFIG);
    }

    static ValueSelector<String> valueSelector(
            ExtractionExpression expression, ConnectorConfig config) throws ExtractionException {
        return new KvpSelectorsSuppliers(config)
                .makeValueSelectorSupplier()
                .newSelector(expression);
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
    public void shouldNotMakeKeySelectorSupplierDueToMissingEvaluatorType() {
        // Configure the key evaluator type, but leave default settings for
        // RECORD_KEY_EVALUATOR_TYPE (String)
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        KvpSelectorsSuppliers s = new KvpSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeKeySelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not KVP");
    }

    @Test
    public void shouldMakeKeySelector() throws ExtractionException {
        KeySelector<String> selector = keySelector(Expression("KEY"));
        assertThat(selector.expression().expression()).isEqualTo("KEY");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,       EXPECTED_ERROR_MESSAGE
                KEY.a. .b,        Found the invalid expression [KEY.a. .b] with missing tokens
                KEY.attrib[],     Found the invalid indexed expression [KEY.attrib[]]
                KEY.attrib[0]xsd, Found the invalid indexed expression [KEY.attrib[0]xsd]
                KEY.attrib[],     Found the invalid indexed expression [KEY.attrib[]]
                KEY.attrib[a],    Found the invalid indexed expression [KEY.attrib[a]]
                    """)
    public void shouldNotCreateKeySelector(String expressionStr, String expectedErrorMessage) {
        ExtractionExpression expression = Expression(expressionStr);
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldMakeValueSelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, KVP.toString()));
        KvpSelectorsSuppliers s = new KvpSelectorsSuppliers(config);
        assertDoesNotThrow(() -> s.makeValueSelectorSupplier());
    }

    @Test
    public void shouldNotMakeValueSelectorSupplierDueToMissingEvaluatorType() {
        // Configure the key evaluator type, but leave default settings for
        // RECORD_KEY_EVALUATOR_TYPE (String)
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        KvpSelectorsSuppliers s = new KvpSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeValueSelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not KVP");
    }

    @Test
    public void shouldMakeValueSelector() throws ExtractionException {
        ValueSelector<String> selector = valueSelector(Expression("VALUE"));
        assertThat(selector.expression().expression()).isEqualTo("VALUE");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,         EXPECTED_ERROR_MESSAGE
                VALUE.a. .b,        Found the invalid expression [VALUE.a. .b] with missing tokens
                VALUE.attrib[],     Found the invalid indexed expression [VALUE.attrib[]]
                VALUE.attrib[0]xsd, Found the invalid indexed expression [VALUE.attrib[0]xsd]
                VALUE.attrib[],     Found the invalid indexed expression [VALUE.attrib[]]
                VALUE.attrib[a],    Found the invalid indexed expression [VALUE.attrib[a]]
                    """)
    public void shouldNotCreateValueSelector(String expressionStr, String expectedErrorMessage) {
        ExtractionExpression expression = Expression(expressionStr);
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldGetDeserializer() {
        Deserializer<String> keyDeserializer =
                new KvpSelectorsSuppliers(CONFIG).makeKeySelectorSupplier().deserializer();
        assertThat(keyDeserializer).isInstanceOf(StringDeserializer.class);

        Deserializer<String> valueDeserializer =
                new KvpSelectorsSuppliers(CONFIG).makeValueSelectorSupplier().deserializer();
        assertThat(valueDeserializer).isInstanceOf(StringDeserializer.class);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION         | EXPECTED_NAME       | EXPECTED_VALUE
                VALUE.QCHARTTOT    | QCHARTTOT          | 2032
                VALUE['QCHARTTOT'] | QCHARTTOT          | 2032
                VALUE.TRow         | TRow               | 12790
                VALUE.QV           | QV                 | 9
                VALUE.PV           | PV                 | 43
                VALUE.TMSTMP       | TMSTMP             | 2024-04-3013:23:07
                VALUE.QCHART       | QCHART             | 1
                VALUE.VTOT         | VTOT               | 81316
                VALUE.QTOT         | QTOT               | 2032
                VALUE.O            | O                  | 30/04/2024-13:23:07
                VALUE.QA           | QA                 | 9012
                VALUE.Q            | Q                  | 1
                VALUE.PA           | PA                 | 40
                VALUE.PCHART       | PCHART             | 43
                VALUE.NTRAD        | NTRAD              | 106
                VALUE.NOVALUE      | NOVALUE            | ''
                VALUE.NOVALUE2     | NOVALUE2           | ''
                    """)
    public void shouldExtractValue(String expressionStr, String expectedName, String expectedValue)
            throws ExtractionException {
        ExtractionExpression expression = Expression(expressionStr);
        ValueSelector<String> valueSelector = valueSelector(expression);

        Data autoBoundData = valueSelector.extractValue(fromValue(INPUT));
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData = valueSelector.extractValue("param", fromValue(INPUT));
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldExtractValueIntoMap() throws ValueException, ExtractionException {
        Map<String, String> target = new HashMap<>();
        KafkaRecord<?, String> record = fromValue(INPUT);

        valueSelector(Expression("VALUE")).extractValueInto(record, target);
        assertThat(target)
                .containsExactly(
                        "QCHARTTOT",
                        "2032",
                        "TRow",
                        "12790",
                        "QV",
                        "9",
                        "PV",
                        "43",
                        "TMSTMP",
                        "2024-04-3013:23:07",
                        "QCHART",
                        "1",
                        "VTOT",
                        "81316",
                        "QTOT",
                        "2032",
                        "O",
                        "30/04/2024-13:23:07",
                        "QA",
                        "9012",
                        "Q",
                        "1",
                        "PA",
                        "40",
                        "PCHART",
                        "43",
                        "NTRAD",
                        "106",
                        "NOVALUE",
                        "",
                        "NOVALUE2",
                        "");
        target.clear();

        valueSelector(Expression("VALUE.QCHART")).extractValueInto(record, target);
        assertThat(target).isEmpty();
        target.clear();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                | EXPECTED_ERROR_MESSAGE
                VALUE                     | The expression [VALUE] must evaluate to a non-complex object
                VALUE.no_attrib           | Field [no_attrib] not found
                VALUE['no_attrib']        | Field [no_attrib] not found
                VALUE.no_children[0]      | Field [no_children] not found
                VALUE.QCHARTTOT[0]        | Field [QCHARTTOT] is not indexed
                VALUE.QCHARTTOT['no_key'] | Cannot retrieve field [no_key] from a scalar object
                VALUE.QCHARTTOT.no_key    | Cannot retrieve field [no_key] from a scalar object
                VALUE.NOVALUE.no_key      | Cannot retrieve field [no_key] from a scalar object
                    """)
    public void shouldNotExtractValue(String expressionStr, String errorMessage)
            throws ValueException, ExtractionException {
        ExtractionExpression expression = Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue("param", fromValue(INPUT))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
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
                EXPRESSION                | EXPECTED_ERROR_MESSAGE
                VALUE.no_attrib           | Field [no_attrib] not found
                VALUE['no_attrib']        | Field [no_attrib] not found
                VALUE.no_children[0]      | Field [no_children] not found
                VALUE.QCHARTTOT[0]        | Field [QCHARTTOT] is not indexed
                VALUE.QCHARTTOT['no_key'] | Cannot retrieve field [no_key] from a scalar object
                VALUE.QCHARTTOT.no_key    | Cannot retrieve field [no_key] from a scalar object
                VALUE.NOVALUE.no_key      | Cannot retrieve field [no_key] from a scalar object
                    """)
    public void shouldNotExtractValueIntoMap(String expressionStr, String errorMessage)
            throws ValueException, ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValueInto(fromValue(INPUT), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @Test
    public void shouldExtractValueWithNonScalars() throws ExtractionException {
        ValueSelector<String> valueSelector = valueSelector(Expression("VALUE"));

        Data autoBoundData = valueSelector.extractValue(fromValue("A=1,B=2"), false);
        assertThat(autoBoundData.name()).isEqualTo("VALUE");
        assertThat(autoBoundData.text()).isEqualTo("{A=1, B=2}");

        Data boundData = valueSelector.extractValue("param", fromValue("A=1,B=2"), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo("{A=1, B=2}");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION | EXPECTED_NAME | EXPECTED_VALUE
                VALUE      | VALUE         | {A=1, B=2}
                VALUE.A    | A             | 1
                VALUE.B    | B             | 2
                    """)
    public void shouldExtractValueWithNonDefaultSettings(
            String expressionString, String expectedName, String expectedValue)
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
        ValueSelector<String> valueSelector = valueSelector(Expression(expressionString), config);

        Data autoBoundData = valueSelector.extractValue(fromValue(message), false);
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData = valueSelector.extractValue("param", fromValue(message), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(expectedValue);

        // Test extractIntoMap as well only for the full object case
        if (expectedName.equals("VALUE")) {
            Map<String, String> target = new HashMap<>();
            KafkaRecord<?, String> record = fromValue(message);
            valueSelector.extractValueInto(record, target);
            assertThat(target).containsExactly("A", "1", "B", "2");
            target.clear();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION | EXPECTED_ERROR_MESSAGE
                VALUE.A    | Cannot retrieve field [VALUE] from a null object
                VALUE.B    | Cannot retrieve field [VALUE] from a null object
                    """)
    public void shouldHandleNullValue(String expressionStr, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValue(fromValue((String) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValue("param", fromValue((String) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValueInto(
                                                fromValue((String) null), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION       | EXPECTED_NAME      | EXPECTED_VALUE
                KEY.QCHARTTOT    | QCHARTTOT          | 2032
                KEY['QCHARTTOT'] | QCHARTTOT          | 2032
                KEY.TRow         | TRow               | 12790
                KEY.QV           | QV                 | 9
                KEY.PV           | PV                 | 43
                KEY.TMSTMP       | TMSTMP             | 2024-04-3013:23:07
                KEY.QCHART       | QCHART             | 1
                KEY.VTOT         | VTOT               | 81316
                KEY.QTOT         | QTOT               | 2032
                KEY.O            | O                  | 30/04/2024-13:23:07
                KEY.QA           | QA                 | 9012
                KEY.Q            | Q                  | 1
                KEY.PA           | PA                 | 40
                KEY.PCHART       | PCHART             | 43
                KEY.NTRAD        | NTRAD              | 106
                KEY.NOVALUE      | NOVALUE            | ''
                KEY.NOVALUE2     | NOVALUE2           | ''
                    """)
    public void shouldExtractKey(String expressionStr, String expectedName, String expectedValue)
            throws ExtractionException {
        ExtractionExpression expression = Expression(expressionStr);
        KeySelector<String> keySelector = keySelector(expression);

        Data autoBoundData = keySelector.extractKey(fromKey(INPUT));
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData = keySelector.extractKey("param", fromKey(INPUT));
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldExtractKeyIntoMap() throws ValueException, ExtractionException {
        Map<String, String> target = new HashMap<>();
        KafkaRecord<String, ?> record = fromKey(INPUT);

        keySelector(Expression("KEY")).extractKeyInto(record, target);
        assertThat(target)
                .containsExactly(
                        "QCHARTTOT",
                        "2032",
                        "TRow",
                        "12790",
                        "QV",
                        "9",
                        "PV",
                        "43",
                        "TMSTMP",
                        "2024-04-3013:23:07",
                        "QCHART",
                        "1",
                        "VTOT",
                        "81316",
                        "QTOT",
                        "2032",
                        "O",
                        "30/04/2024-13:23:07",
                        "QA",
                        "9012",
                        "Q",
                        "1",
                        "PA",
                        "40",
                        "PCHART",
                        "43",
                        "NTRAD",
                        "106",
                        "NOVALUE",
                        "",
                        "NOVALUE2",
                        "");
        target.clear();

        keySelector(Expression("KEY.QCHART")).extractKeyInto(record, target);
        assertThat(target).isEmpty();
        target.clear();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION              | EXPECTED_ERROR_MESSAGE
                KEY                     | The expression [KEY] must evaluate to a non-complex object
                KEY.no_attrib           | Field [no_attrib] not found
                KEY['no_attrib']        | Field [no_attrib] not found
                KEY.no_children[0]      | Field [no_children] not found
                KEY.QCHARTTOT[0]        | Field [QCHARTTOT] is not indexed
                KEY.QCHARTTOT['no_key'] | Cannot retrieve field [no_key] from a scalar object
                KEY.QCHARTTOT.no_key    | Cannot retrieve field [no_key] from a scalar object
                KEY.NOVALUE.no_key      | Cannot retrieve field [no_key] from a scalar object
                    """)
    public void shouldNotExtractKey(String expressionStr, String errorMessage) {
        ExtractionExpression expression = Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> keySelector(expression).extractKey("param", fromKey(INPUT)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
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
                EXPRESSION              | EXPECTED_ERROR_MESSAGE
                KEY.no_attrib           | Field [no_attrib] not found
                KEY['no_attrib']        | Field [no_attrib] not found
                KEY.no_children[0]      | Field [no_children] not found
                KEY.QCHARTTOT[0]        | Field [QCHARTTOT] is not indexed
                KEY.QCHARTTOT['no_key'] | Cannot retrieve field [no_key] from a scalar object
                KEY.QCHARTTOT.no_key    | Cannot retrieve field [no_key] from a scalar object
                KEY.NOVALUE.no_key      | Cannot retrieve field [no_key] from a scalar object
                    """)
    public void shouldNotExtractKeyIntoMap(String expressionStr, String errorMessage)
            throws ValueException, ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKeyInto(fromKey(INPUT), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @Test
    public void shouldExtractKeyWithNonScalars() throws ExtractionException {
        KeySelector<String> keySelector = keySelector(Expression("KEY"));

        Data autoBoundData = keySelector.extractKey(fromKey("A=1,B=2"), false);
        assertThat(autoBoundData.name()).isEqualTo("KEY");
        assertThat(autoBoundData.text()).isEqualTo("{A=1, B=2}");

        Data boundData = keySelector.extractKey("param", fromKey("A=1,B=2"), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo("{A=1, B=2}");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION | EXPECTED_NAME | EXPECTED_VALUE
                KEY        | KEY         | {A=1, B=2}
                KEY.A      | A             | 1
                KEY.B      | B             | 2
                    """)
    public void shouldExtractKeyWithNonDefaultSettings(
            String expressionString, String expectedName, String expectedValue)
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
        KeySelector<String> keySelector = keySelector(Expression(expressionString), config);

        Data autoBoundData = keySelector.extractKey(fromKey(message), false);
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData = keySelector.extractKey("param", fromKey(message), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(expectedValue);

        // Test extractIntoMap as well only for the full object case
        if (expectedName.equals("VALUE")) {
            Map<String, String> target = new HashMap<>();
            KafkaRecord<String, ?> record = fromKey(message);
            keySelector.extractKeyInto(record, target);
            assertThat(target).containsExactly("A", "1", "B", "2");
            target.clear();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION | EXPECTED_ERROR_MESSAGE
                KEY.A      | Cannot retrieve field [KEY] from a null object
                KEY.B      | Cannot retrieve field [KEY] from a null object
                    """)
    public void shouldHandleNullKey(String expressionStr, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKey(fromKey((String) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKey("param", fromKey((String) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKeyInto(fromKey((String) null), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }
}
