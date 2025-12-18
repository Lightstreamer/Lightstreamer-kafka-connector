
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedNoWildcardCheck;
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;
import static com.lightstreamer.kafka.test_utils.SampleMessageProviders.SampleJsonNodeProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
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

import java.util.HashMap;
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

    static KeySelector<JsonNode> keySelector(String expression) throws ExtractionException {
        return new JsonNodeSelectorsSuppliers(CONFIG)
                .makeKeySelectorSupplier()
                .newSelector(WrappedNoWildcardCheck("#{" + expression + "}"));
    }

    static ValueSelector<JsonNode> valueSelector(String expression) throws ExtractionException {
        return new JsonNodeSelectorsSuppliers(CONFIG)
                .makeValueSelectorSupplier()
                .newSelector(WrappedNoWildcardCheck("#{" + expression + "}"));
    }

    @Test
    public void shouldMakeKeySelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, JSON.toString()));
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers(config);
        KeySelectorSupplier<JsonNode> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.evaluatorType()).isEqualTo(EvaluatorType.JSON);
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
        assertThat(ie).hasMessageThat().isEqualTo("Evaluator type is not JSON");
    }

    @Test
    public void shouldMakeKeySelectorSupplierWithNoConfig() {
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers();
        KeySelectorSupplier<JsonNode> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.deserializer().getClass())
                .isEqualTo(KafkaJsonDeserializer.class);
    }

    @Test
    public void shouldMakeKeySelector() throws ExtractionException {
        KeySelector<JsonNode> selector = keySelector("KEY");
        assertThat(selector.expression().expression()).isEqualTo("KEY");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION       | EXPECTED_ERROR_MESSAGE
                KEY.attrib[]     | Found the invalid indexed expression [KEY.attrib[]]
                KEY.attrib[0]xsd | Found the invalid indexed expression [KEY.attrib[0]xsd]
                KEY.attrib[]     | Found the invalid indexed expression [KEY.attrib[]]
                KEY.attrib[a]    | Found the invalid indexed expression [KEY.attrib[a]]
                    """)
    public void shouldNotMakeKeySelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> keySelector(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldMakeValueSelectorSupplier() throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, JSON.toString()));
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers(config);
        ValueSelectorSupplier<JsonNode> valueSelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(valueSelectorSupplier.evaluatorType()).isEqualTo(EvaluatorType.JSON);
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
        assertThat(ie).hasMessageThat().isEqualTo("Evaluator type is not JSON");
    }

    @Test
    public void shouldMakeValueSelectorSupplierWithNoConfig() {
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers();
        ValueSelectorSupplier<JsonNode> valueSelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(valueSelectorSupplier.deserializer().getClass())
                .isEqualTo(KafkaJsonDeserializer.class);
    }

    @Test
    public void shouldMakeValueSelector() throws ExtractionException {
        ValueSelector<JsonNode> selector = valueSelector("VALUE");
        assertThat(selector.expression().expression()).isEqualTo("VALUE");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION         | EXPECTED_ERROR_MESSAGE
                VALUE.attrib[]     | Found the invalid indexed expression [VALUE.attrib[]]
                VALUE.attrib[0]xsd | Found the invalid indexed expression [VALUE.attrib[0]xsd]
                VALUE.attrib[]     | Found the invalid indexed expression [VALUE.attrib[]]
                VALUE.attrib[a]    | Found the invalid indexed expression [VALUE.attrib[a]]
                    """)
    public void shouldNotMakeValueSelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> valueSelector(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldGetDeserializer() {
        Deserializer<JsonNode> keyDeserializer =
                new JsonNodeSelectorsSuppliers(CONFIG).makeKeySelectorSupplier().deserializer();
        assertThat(keyDeserializer).isInstanceOf(KafkaJsonDeserializer.class);

        Deserializer<JsonNode> valueDeserializer =
                new JsonNodeSelectorsSuppliers(CONFIG).makeValueSelectorSupplier().deserializer();
        assertThat(valueDeserializer).isInstanceOf(KafkaJsonDeserializer.class);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                            | EXPECTED_NAME        | EXPECTED_VALUE
                VALUE.name                            | name                 | joe
                VALUE['name']                         | name                 | joe
                VALUE.signature                       | signature            | YWJjZA==
                VALUE['notes'][0]                     | notes[0]             | note1
                VALUE.notes[0]                        | notes[0]             | note1
                VALUE.stringsStrings[0][1]            | stringsStrings[0][1] | ss01
                VALUE['stringsStrings'][0][1]         | stringsStrings[0][1] | ss01
                VALUE.mapsMaps.map1.m11               | m11                  | mv11
                VALUE.mapsMaps['map1']['m11']         | m11                  | mv11
                VALUE.mapsMaps['map1'].m11            | m11                  | mv11
                VALUE['mapsMaps']['map1']['m11']      | m11                  | mv11
                VALUE['mapsMaps'].map1['m11']         | m11                  | mv11
                VALUE.children[0].name                | name                 | alex
                VALUE.children[0]['name']             | name                 | alex
                VALUE.children[0].signature           | signature            |
                VALUE.children[1].name                | name                 | anna
                VALUE.children[2].name                | name                 | serena
                VALUE.children[3]                     | children[3]          |
                VALUE.children[1].children[0].name    | name                 | gloria
                VALUE.children[1].children[1].name    | name                 | terence
                VALUE.children[1].children[1]['name'] | name                 | terence
                VALUE.family[0][0].name               | name                 | bro00
                VALUE.family[0][1].name               | name                 | bro01
                VALUE.family[1][0].name               | name                 | bro10
                VALUE.family[1][1].name               | name                 | bro11
                    """)
    public void shouldExtractValue(String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        ValueSelector<JsonNode> valueSelector = valueSelector(expression);

        Data autoBoundData = valueSelector.extractValue(fromValue(SAMPLE_MESSAGE));
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData = valueSelector.extractValue("param", fromValue(SAMPLE_MESSAGE), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldExtractValueIntoMap()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        JsonNode message =
                om.readTree(
                        """
                    {
                         "name": "joe",
                         "signature": "YWJjZA",
                         "data": [1, 2, 3, null],
                         "emptyData": [],
                         "map": {"a": 1, "b": 2, "c": 3},
                         "emptyMap": {},
                         "nullValue": null,
                         "matrix": [
                             [1,2,3],
                             [4,5,6],
                             [7,8,9]
                         ]
                         }
                    }
                        """);

        Map<String, String> target = new HashMap<>();
        KafkaRecord<?, JsonNode> record = fromValue(message);

        valueSelector("VALUE.*").extractValueInto(record, target);
        assertThat(target)
                .containsExactly(
                        "name",
                        "joe",
                        "signature",
                        "YWJjZA",
                        "data",
                        "[1,2,3,null]",
                        "emptyData",
                        "[]",
                        "map",
                        "{\"a\":1,\"b\":2,\"c\":3}",
                        "emptyMap",
                        "{}",
                        "matrix",
                        "[[1,2,3],[4,5,6],[7,8,9]]",
                        "nullValue",
                        null);
        target.clear();

        valueSelector("VALUE.data").extractValueInto(record, target);
        assertThat(target)
                .containsExactly("data[0]", "1", "data[1]", "2", "data[2]", "3", "data[3]", null);
        target.clear();

        valueSelector("VALUE.emptyData").extractValueInto(record, target);
        assertThat(target).isEmpty();
        target.clear();

        valueSelector("VALUE.map").extractValueInto(record, target);
        assertThat(target).containsExactly("a", "1", "b", "2", "c", "3");
        target.clear();

        valueSelector("VALUE.emptyMap").extractValueInto(record, target);
        assertThat(target).isEmpty();
        target.clear();

        valueSelector("VALUE.matrix").extractValueInto(record, target);
        assertThat(target)
                .containsExactly(
                        "matrix[0]", "[1,2,3]", "matrix[1]", "[4,5,6]", "matrix[2]", "[7,8,9]");
        target.clear();

        valueSelector("VALUE.matrix[1]").extractValueInto(record, target);
        assertThat(target)
                .containsExactly("matrix[1][0]", "4", "matrix[1][1]", "5", "matrix[1][2]", "6");
        target.clear();

        valueSelector("VALUE.nullValue").extractValueInto(record, target);
        assertThat(target).isEmpty();
        target.clear();

        valueSelector("VALUE.nullValue").extractValueInto(record, target);
        assertThat(target).isEmpty();
        target.clear();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                  | EXPECTED_ERROR_MESSAGE
                VALUE                       | The expression [VALUE] must evaluate to a non-complex object
                VALUE.a b                   | Field [a b] not found
                VALUE.no_attrib             | Field [no_attrib] not found
                VALUE['no_attrib']          | Field [no_attrib] not found
                VALUE[0]                    | Cannot retrieve index [0] from a non-array object
                VALUE.children[0].no_attrib | Field [no_attrib] not found
                VALUE.no_children[0]        | Field [no_children] not found
                VALUE.name[0]               | Field [name] is not indexed
                VALUE.name['no_key']        | Cannot retrieve field [no_key] from a scalar object
                VALUE.name.no_key           | Cannot retrieve field [no_key] from a scalar object
                VALUE.children              | The expression [VALUE.children] must evaluate to a non-complex object
                VALUE.children[0]['no_key'] | Field [no_key] not found
                VALUE.children['name']      | Cannot retrieve field [name] from an array object
                VALUE.children.name         | Cannot retrieve field [name] from an array object
                VALUE.children[0]           | The expression [VALUE.children[0]] must evaluate to a non-complex object
                VALUE.children[3].name      | Cannot retrieve field [name] from a null object
                VALUE.children[4]           | Field not found at index [4]
                VALUE.children[4].name      | Field not found at index [4]
                VALUE.nullArray[0]          | Cannot retrieve index [0] from a null object
                VALUE.*                     | The expression [VALUE.*] must evaluate to a non-complex object
                    """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue("param", fromValue(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue(fromValue(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                  | EXPECTED_ERROR_MESSAGE
                VALUE.no_attrib             | Field [no_attrib] not found
                VALUE['no_attrib']          | Field [no_attrib] not found
                VALUE.children[0].no_attrib | Field [no_attrib] not found
                VALUE.no_children[0]        | Field [no_children] not found
                VALUE.name[0]               | Field [name] is not indexed
                VALUE.name['no_key']        | Cannot retrieve field [no_key] from a scalar object
                VALUE.name.no_key           | Cannot retrieve field [no_key] from a scalar object
                VALUE.children[0]['no_key'] | Field [no_key] not found
                VALUE.children[3].name      | Cannot retrieve field [name] from a null object
                VALUE.children[4]           | Field not found at index [4]
                VALUE.children[4].name      | Field not found at index [4]
                VALUE.nullArray[0]          | Cannot retrieve index [0] from a null object
                    """)
    public void shouldNotExtractValueIntoMap(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValueInto(
                                                fromValue(SAMPLE_MESSAGE), new HashMap<>()));
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION             | EXPECTED_NAME | EXPECTED_VALUE
                VALUE                  | VALUE         | {"root":{"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2},"nullValue":null}}
                VALUE.root             | root          | {"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2},"nullValue":null}
                VALUE.root.name        | name          | joe
                VALUE.root.emptyArray  | emptyArray    | []
                VALUE.root.array       | array         | [1,2,3]
                VALUE.root.emptyObject | emptyObject   | {}
                VALUE.root.object      | object        | {"a":1,"b":2}
                    """)
    public void shouldExtractValueWithNonScalars(
            String expressionString, String expectedName, String expectedValue)
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
                            "array": [1,2,3],
                            "emptyObject": {},
                            "object": {
                                "a": 1,
                                "b": 2
                             },
                             "nullValue": null
                        }
                    }
                        """);

        ValueSelector<JsonNode> valueSelector = valueSelector(expressionString);

        Data autoBoundData = valueSelector.extractValue(fromValue(message), false);
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData =
                valueSelector(expressionString).extractValue("param", fromValue(message), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldHandleNullValue() throws ExtractionException {
        ValueSelector<JsonNode> valueSelector = valueSelector("VALUE");

        Data autoBoundData = valueSelector.extractValue(fromValue((JsonNode) null), false);
        assertThat(autoBoundData.name()).isEqualTo("VALUE");
        assertThat(autoBoundData.text()).isNull();

        Data boundData = valueSelector.extractValue("param", fromValue((JsonNode) null), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isNull();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                  | EXPECTED_ERROR_MESSAGE
                VALUE.no_attrib             | Cannot retrieve field [no_attrib] from a null object
                VALUE.children[0].no_attrib | Cannot retrieve field [children] from a null object
                VALUE.no_children[0]        | Cannot retrieve field [no_children] from a null object
                    """)
    public void shouldNotExtractFromNullValue(String expression, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue(fromValue((JsonNode) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue("param", fromValue((JsonNode) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValueInto(
                                                fromValue((JsonNode) null), new HashMap<>()));
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                          | EXPECTED_NAME | EXPECTED_VALUE
                KEY.name                            | name          | joe
                KEY['name']                         | name          | joe
                KEY.signature                       | signature     | YWJjZA==
                KEY.notes[0]                        | notes[0]      | note1
                KEY.children[0].name                | name          | alex
                KEY.children[0]['name']             | name          | alex
                KEY.children[0].signature           | signature     |
                KEY.children[1].name                | name          | anna
                KEY.children[2].name                | name          | serena
                KEY.children[3]                     | children[3]   |
                KEY.children[1].children[0].name    | name          | gloria
                KEY.children[1].children[1].name    | name          | terence
                KEY.children[1].children[1]['name'] | name          | terence
                KEY.family[0][0].name               | name          | bro00
                KEY.family[0][1].name               | name          | bro01
                KEY.family[1][0].name               | name          | bro10
                KEY.family[1][1].name               | name          | bro11
                    """)
    public void shouldExtractKey(String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        KeySelector<JsonNode> keySelector = keySelector(expression);

        Data autoBoundData = keySelector.extractKey(fromKey(SAMPLE_MESSAGE), false);
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundValueData = keySelector.extractKey(expectedName, fromKey(SAMPLE_MESSAGE));
        assertThat(boundValueData.name()).isEqualTo(expectedName);
        assertThat(boundValueData.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldExtractKeyIntoMap()
            throws ExtractionException, JsonMappingException, JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        JsonNode message =
                om.readTree(
                        """
                    {
                         "name": "joe",
                         "signature": "YWJjZA",
                         "data": [1, 2, 3, null],
                         "emptyData": [],
                         "map": {"a": 1, "b": 2, "c": 3},
                         "emptyMap": {},
                         "nullValue": null
                    }
                        """);

        Map<String, String> target = new HashMap<>();
        KafkaRecord<JsonNode, ?> record = fromKey(message);

        keySelector("KEY.*").extractKeyInto(record, target);
        assertThat(target)
                .containsExactly(
                        "name",
                        "joe",
                        "signature",
                        "YWJjZA",
                        "data",
                        "[1,2,3,null]",
                        "emptyData",
                        "[]",
                        "map",
                        "{\"a\":1,\"b\":2,\"c\":3}",
                        "emptyMap",
                        "{}",
                        "nullValue",
                        null);
        target.clear();

        keySelector("KEY.data").extractKeyInto(record, target);
        assertThat(target)
                .containsExactly("data[0]", "1", "data[1]", "2", "data[2]", "3", "data[3]", null);
        target.clear();

        keySelector("KEY.emptyData").extractKeyInto(record, target);
        assertThat(target).isEmpty();
        target.clear();

        keySelector("KEY.map").extractKeyInto(record, target);
        assertThat(target).containsExactly("a", "1", "b", "2", "c", "3");
        target.clear();

        keySelector("KEY.emptyMap").extractKeyInto(record, target);
        assertThat(target).isEmpty();
        target.clear();

        keySelector("KEY.nullValue").extractKeyInto(record, target);
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
                KEY                       | The expression [KEY] must evaluate to a non-complex object
                KEY.a b                   | Field [a b] not found
                KEY.no_attrib             | Field [no_attrib] not found
                KEY['no_attrib']          | Field [no_attrib] not found
                KEY[0]                    | Cannot retrieve index [0] from a non-array object
                KEY.children[0].no_attrib | Field [no_attrib] not found
                KEY.no_children[0]        | Field [no_children] not found
                KEY.name[0]               | Field [name] is not indexed
                KEY.name['no_key']        | Cannot retrieve field [no_key] from a scalar object
                KEY.name.no_key           | Cannot retrieve field [no_key] from a scalar object
                KEY.children              | The expression [KEY.children] must evaluate to a non-complex object
                KEY.children.name         | Cannot retrieve field [name] from an array object
                KEY.children['name']      | Cannot retrieve field [name] from an array object
                KEY.children[0]['no_key'] | Field [no_key] not found
                KEY.children[0]           | The expression [KEY.children[0]] must evaluate to a non-complex object
                KEY.children[3].name      | Cannot retrieve field [name] from a null object
                KEY.children[4]           | Field not found at index [4]
                KEY.children[4].name      | Field not found at index [4]
                KEY.nullArray[0]          | Cannot retrieve index [0] from a null object
                KEY.*                     | The expression [KEY.*] must evaluate to a non-complex object
                    """)
    public void shouldNotExtractKey(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKey("param", fromKey(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () -> keySelector(expression).extractKey(fromKey(SAMPLE_MESSAGE)).text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                | EXPECTED_ERROR_MESSAGE
                #KEY.no_attrib             | Field [no_attrib] not found
                #KEY['no_attrib']          | Field [no_attrib] not found
                #KEY.children[0].no_attrib | Field [no_attrib] not found
                #KEY.no_children[0]        | Field [no_children] not found
                #KEY.name[0]               | Field [name] is not indexed
                #KEY.name['no_key']        | Cannot retrieve field [no_key] from a scalar object
                #KEY.name.no_key           | Cannot retrieve field [no_key] from a scalar object
                #KEY.children[0]['no_key'] | Field [no_key] not found
                KEY.children[3].name      | Cannot retrieve field [name] from a null object
                #KEY.children[4]           | Field not found at index [4]
                #KEY.children[4].name      | Field not found at index [4]
                #KEY.nullArray[0]          | Cannot retrieve index [0] from a null object
                    """)
    public void shouldNotExtractKeyIntoMap(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKeyInto(fromKey(SAMPLE_MESSAGE), new HashMap<>()));
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION           | EXPECTED_NAME | EXPECTED_VALUE
                KEY                  | KEY           | {"root":{"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2}}}
                KEY.root             | root          | {"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2}}
                KEY.root.name        | name          | joe
                KEY.root.emptyArray  | emptyArray    | []
                KEY.root.array       | array         | [1,2,3]
                KEY.root.emptyObject | emptyObject   | {}
                KEY.root.object      | object        | {"a":1,"b":2}
                    """)
    public void shouldExtractKeyWithNonScalars(
            String expression, String expectedName, String expectedValue)
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
                            "array": [1,2,3],
                            "emptyObject": {},
                            "object": {
                                "a": 1,
                                "b": 2
                             }
                        }
                    }
                        """);

        Data autoBoundValue = keySelector(expression).extractKey(fromKey(message), false);
        assertThat(autoBoundValue.name()).isEqualTo(expectedName);
        assertThat(autoBoundValue.text()).isEqualTo(expectedValue);

        Data boundValue = keySelector(expression).extractKey("param", fromKey(message), false);
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldHandleNullKey() throws ExtractionException {
        KeySelector<JsonNode> keySelector = keySelector("KEY");

        Data autoBoundData = keySelector.extractKey(fromKey((JsonNode) null), false);
        assertThat(autoBoundData.name()).isEqualTo("KEY");
        assertThat(autoBoundData.text()).isNull();

        Data boundData = keySelector.extractKey("param", fromKey((JsonNode) null), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isNull();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                | EXPECTED_ERROR_MESSAGE
                KEY.no_attrib             | Cannot retrieve field [no_attrib] from a null object
                KEY.children[0].no_attrib | Cannot retrieve field [children] from a null object
                KEY.no_children[0]        | Cannot retrieve field [no_children] from a null object
                    """)
    public void shouldHandleNullKey(String expression, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> keySelector(expression).extractKey(fromKey((JsonNode) null)).text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKey("param", fromKey((JsonNode) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKeyInto(fromKey((JsonNode) null), new HashMap<>()));
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }
}
