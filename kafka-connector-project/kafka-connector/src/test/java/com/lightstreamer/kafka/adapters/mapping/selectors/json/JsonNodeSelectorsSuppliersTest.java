
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

package com.lightstreamer.kafka.adapters.mapping.selectors.json;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.JSON;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

public class JsonNodeSelectorsSuppliersTest {

    // A configuration with proper evaluator type settings for key and value
    static ConnectorConfig CONFIG =
            ConnectorConfigProvider.minimalWith(
                    Map.of(
                            RECORD_KEY_EVALUATOR_TYPE,
                            JSON.toString(),
                            RECORD_VALUE_EVALUATOR_TYPE,
                            JSON.toString()));
    static JsonNode SAMPLE_MESSAGE = SampleJsonNodeProvider().sampleMessage();

    static ValueSelector<JsonNode> valueSelector(ExtractionExpression expression)
            throws ExtractionException {
        return new JsonNodeSelectorsSuppliers(CONFIG)
                .makeValueSelectorSupplier()
                .newSelector("name", expression);
    }

    static KeySelector<JsonNode> keySelector(ExtractionExpression expression)
            throws ExtractionException {
        return new JsonNodeSelectorsSuppliers(CONFIG)
                .makeKeySelectorSupplier()
                .newSelector("name", expression);
    }

    @Test
    public void shouldMakeKeySelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, JSON.toString()));
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers(config);
        assertDoesNotThrow(() -> s.makeKeySelectorSupplier());
    }

    @Test
    public void shouldNotMakeKeySelectorSupplierDueToMissingEvaluatorType() {
        // Configure the key evaluator type, but leave default settings for
        // RECORD_KEY_EVALUATOR_TYPE (String)
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, JSON.toString()));
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeKeySelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not JSON");
    }

    @Test
    public void shouldMakeKeySelectorSupplierWithNoConfig() {
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers();
        KeySelectorSupplier<JsonNode> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.deserializer().getClass())
                .isEqualTo(KafkaJsonDeserializer.class);
    }

    @Test
    public void shouldMakeValueSelectorSupplierWithNoConfig() {
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers();
        ValueSelectorSupplier<JsonNode> valueSelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(valueSelectorSupplier.deserializer().getClass())
                .isEqualTo(KafkaJsonDeserializer.class);
    }

    @Test
    public void shouldMakeValueSelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, JSON.toString()));
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers(config);
        assertDoesNotThrow(() -> s.makeValueSelectorSupplier());
    }

    @Test
    public void shouldNotMakeValueSelectorSupplierDueToMissingEvaluatorType() {
        // Configure the key evaluator type, but leave default settings for
        // RECORD_VALUE_EVALUATOR_TYPE (String)
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, JSON.toString()));
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeValueSelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not JSON");
    }

    @Test
    public void shouldGetDeserializer() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, JSON.toString()));

        Deserializer<JsonNode> keyDeserializer =
                new JsonNodeSelectorsSuppliers(config).makeKeySelectorSupplier().deserializer();
        assertThat(keyDeserializer).isInstanceOf(KafkaJsonDeserializer.class);

        Deserializer<JsonNode> valueDeserializer =
                new JsonNodeSelectorsSuppliers(CONFIG).makeValueSelectorSupplier().deserializer();
        assertThat(valueDeserializer).isInstanceOf(KafkaJsonDeserializer.class);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                            EXPECTED
                VALUE.name,                            joe
                VALUE.signature,                       YWJjZA==
                VALUE.children[0].name,                alex
                VALUE.children[0]['name'],             alex
                VALUE.children[0].signature,           NULL
                VALUE.children[1].name,                anna
                VALUE.children[2].name,                serena
                VALUE.children[3],                     NULL
                VALUE.children[1].children[0].name,    gloria
                VALUE.children[1].children[1].name,    terence
                VALUE.children[1].children[1]['name'], terence
                VALUE.family[0][0].name,               bro00
                VALUE.family[0][1].name,               bro01
                VALUE.family[1][0].name,               bro10
                VALUE.family[1][1].name,               bro11
                    """)
    public void shouldExtractValue(String expressionStr, String expected)
            throws ExtractionException {
        StringSubject subject =
                assertThat(
                        valueSelector(Expression(expressionStr))
                                .extractValue(fromValue(SAMPLE_MESSAGE))
                                .text());
        if (expected.equals("NULL")) {
            subject.isNull();
        } else {
            subject.isEqualTo(expected);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                  EXPECTED_ERROR_MESSAGE
                VALUE,                       The expression [VALUE] must evaluate to a non-complex object
                VALUE.no_attrib,             Field [no_attrib] not found
                VALUE.children[0].no_attrib, Field [no_attrib] not found
                VALUE.no_children[0],        Field [no_children] not found
                VALUE.name[0],               Field [name] is not indexed
                VALUE.name['no_key'],        Cannot retrieve field [no_key] from a scalar object
                VALUE.name.no_key,           Cannot retrieve field [no_key] from a scalar object
                VALUE.children,              The expression [VALUE.children] must evaluate to a non-complex object
                VALUE.children[0]['no_key'], Field [no_key] not found
                VALUE.children[0],           The expression [VALUE.children[0]] must evaluate to a non-complex object
                VALUE.children[3].name,      Cannot retrieve field [name] from a null object
                VALUE.children[4],           Field not found at index [4]
                VALUE.children[4].name,      Field not found at index [4]
                VALUE.nullArray[0],          Cannot retrieve index [0] from null object [nullArray]
                    """)
    public void shouldNotExtractValue(String expressionStr, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValue(fromValue(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION             | EXPECTED
                VALUE                  | {"root":{"name":"joe","signature":"YWJjZA","emptyArray":[],"emptyObject":{}}}
                VALUE.root             | {"name":"joe","signature":"YWJjZA","emptyArray":[],"emptyObject":{}}
                VALUE.root.name        | joe
                VALUE.root.emptyArray  | []
                VALUE.root.emptyObject | {}
                    """)
    public void shouldExtractValueWithNonScalars(String expressionString, String expected)
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        JsonNode message =
                om.readTree(
                        """
                {
                    "root": {
                        "name": "joe",
                        "signature": "YWJjZA",
                        "emptyArray": [],
                        "emptyObject": {}
                        }
                }
                """);

        String extractedData =
                valueSelector(Expression(expressionString))
                        .extractValue(fromValue(message), false)
                        .text();
        assertThat(extractedData).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                  EXPECTED_ERROR_MESSAGE
                VALUE.no_attrib,             Cannot retrieve field [no_attrib] from a null object
                VALUE.children[0].no_attrib, Cannot retrieve field [children] from a null object
                VALUE.no_children[0],        Cannot retrieve field [no_children] from a null object
                    """)
    public void shouldHandleNullValue(String expressionStr, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValue(fromValue((JsonNode) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                          EXPECTED
                KEY.name,                            joe
                KEY.signature,                       YWJjZA==
                KEY.children[0].name,                alex
                KEY.children[0]['name'],             alex
                KEY.children[0].signature,           NULL
                KEY.children[1].name,                anna
                KEY.children[2].name,                serena
                KEY.children[3],                     NULL
                KEY.children[1].children[0].name,    gloria
                KEY.children[1].children[1].name,    terence
                KEY.children[1].children[1]['name'], terence
                    """)
    public void shouldExtractKey(String expressionStr, String expected) throws ExtractionException {
        StringSubject subject =
                assertThat(
                        keySelector(Expression(expressionStr))
                                .extractKey(fromKey(SAMPLE_MESSAGE))
                                .text());
        if (expected.equals("NULL")) {
            subject.isNull();
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
                KEY,                       The expression [KEY] must evaluate to a non-complex object
                KEY.no_attrib,             Field [no_attrib] not found
                KEY.children[0].no_attrib, Field [no_attrib] not found
                KEY.no_children[0],        Field [no_children] not found
                KEY.name[0],               Field [name] is not indexed
                KEY.name['no_key'],        Cannot retrieve field [no_key] from a scalar object
                KEY.name.no_key,           Cannot retrieve field [no_key] from a scalar object
                KEY.children,              The expression [KEY.children] must evaluate to a non-complex object
                KEY.children[0]['no_key'], Field [no_key] not found
                KEY.children[0],           The expression [KEY.children[0]] must evaluate to a non-complex object
                KEY.children[3].name,      Cannot retrieve field [name] from a null object
                KEY.children[4],           Field not found at index [4]
                KEY.children[4].name,      Field not found at index [4]
                KEY.nullArray[0],          Cannot retrieve index [0] from null object [nullArray]
                    """)
    public void shouldNotExtractKey(String expressionStr, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKey(fromKey(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION           | EXPECTED
                KEY                  | {"root":{"name":"joe","signature":"YWJjZA","emptyArray":[],"emptyObject":{}}}
                KEY.root             | {"name":"joe","signature":"YWJjZA","emptyArray":[],"emptyObject":{}}
                KEY.root.name        | joe
                KEY.root.emptyArray  | []
                KEY.root.emptyObject | {}
                    """)
    public void shouldExtractKeyWithNonScalars(String expressionString, String expected)
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        JsonNode message =
                om.readTree(
                        """
                {
                    "root": {
                        "name": "joe",
                        "signature": "YWJjZA",
                        "emptyArray": [],
                        "emptyObject": {}
                        }
                }
                """);

        String text =
                keySelector(Expression(expressionString))
                        .extractKey(fromKey(message), false)
                        .text();
        assertThat(text).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                  EXPECTED_ERROR_MESSAGE
                KEY.no_attrib,             Cannot retrieve field [no_attrib] from a null object
                KEY.children[0].no_attrib, Cannot retrieve field [children] from a null object
                KEY.no_children[0],        Cannot retrieve field [no_children] from a null object
                    """)
    public void shouldHandleNullKey(String expressionStr, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKey(fromKey((JsonNode) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
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
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class, () -> valueSelector(Expression(expressionStr)));
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
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class, () -> keySelector(Expression(expressionStr)));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
