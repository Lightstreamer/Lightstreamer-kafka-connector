
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
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
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

    static KeySelector<JsonNode> keySelector(ExtractionExpression expression)
            throws ExtractionException {
        return new JsonNodeSelectorsSuppliers(CONFIG)
                .makeKeySelectorSupplier()
                .newSelector(expression);
    }

    static ValueSelector<JsonNode> valueSelector(ExtractionExpression expression)
            throws ExtractionException {
        return new JsonNodeSelectorsSuppliers(CONFIG)
                .makeValueSelectorSupplier()
                .newSelector(expression);
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
    public void shouldMakeKeySelector() throws ExtractionException {
        KeySelector<JsonNode> selector = keySelector(Expression("KEY"));
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
    public void shouldNotMakeKeySelector(String expressionStr, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class, () -> keySelector(Expression(expressionStr)));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @Test
    public void shouldMakeValueSelectorSupplier() throws ExtractionException {
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
    public void shouldMakeValueSelectorSupplierWithNoConfig() {
        JsonNodeSelectorsSuppliers s = new JsonNodeSelectorsSuppliers();
        ValueSelectorSupplier<JsonNode> valueSelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(valueSelectorSupplier.deserializer().getClass())
                .isEqualTo(KafkaJsonDeserializer.class);
    }

    @Test
    public void shouldMakeValueSelector() throws ExtractionException {
        ValueSelector<JsonNode> selector = valueSelector(Expression("VALUE"));
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
    public void shouldNotMakeValueSelector(String expressionStr, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class, () -> valueSelector(Expression(expressionStr)));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
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
                EXPRESSION,                            PARAM,          EXPECTED
                VALUE.name,                            aName,          joe
                VALUE['name'],                         aName,          joe
                VALUE.signature,                       aSignature,     YWJjZA==
                VALUE.notes[0],                        note0,          note1
                VALUE.children[0].name,                childName,      alex
                VALUE.children[0]['name'],             childName,      alex
                VALUE.children[0].signature,           childSignature, NULL
                VALUE.children[1].name,                childName,      anna
                VALUE.children[2].name,                childName,      serena
                VALUE.children[3],                     child,          NULL
                VALUE.children[1].children[0].name,    childName,      gloria
                VALUE.children[1].children[1].name,    childName,      terence
                VALUE.children[1].children[1]['name'], childName,      terence
                VALUE.family[0][0].name,               familyMember,   bro00
                VALUE.family[0][1].name,               familyMember,   bro01
                VALUE.family[1][0].name,               familyMember,   bro10
                VALUE.family[1][1].name,               familyMember,   bro11
                    """)
    public void shouldExtractBoundValue(String expressionStr, String param, String expected)
            throws ExtractionException {
        ValueSelector<JsonNode> valueSelector = valueSelector(Expression(expressionStr));
        Data data = valueSelector.extractValue(param, fromValue(SAMPLE_MESSAGE));
        assertThat(data.name()).isEqualTo(param);
        StringSubject subject = assertThat(data.text());
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
                EXPRESSION,                            EXPECTED_NAME, EXPECTED_VALUE
                VALUE.name,                            name,          joe
                VALUE['name'],                         name,          joe
                VALUE.signature,                       signature,     YWJjZA==
                VALUE.notes[0],                        notes[0],      note1
                VALUE.children[0].name,                name,          alex
                VALUE.children[0]['name'],             name,          alex
                VALUE.children[0].signature,           signature,     NULL
                VALUE.children[1].name,                name,          anna
                VALUE.children[2].name,                name,          serena
                VALUE.children[3],                     children[3]    NULL
                VALUE.children[1].children[0].name,    name,          gloria
                VALUE.children[1].children[1].name,    name,          terence
                VALUE.children[1].children[1]['name'], name,          terence
                VALUE.family[0][0].name,               name,          bro00
                VALUE.family[0][1].name,               name,          bro01
                VALUE.family[1][0].name,               name,          bro10
                VALUE.family[1][1].name,               name,          bro11
                    """)
    public void shouldExtractAutoBoundValue(
            String expressionStr, String expectedName, String expectedValue)
            throws ExtractionException {
        ValueSelector<JsonNode> valueSelector = valueSelector(Expression(expressionStr));
        Data data = valueSelector.extractValue(fromValue(SAMPLE_MESSAGE), false);
        assertThat(data.name()).isEqualTo(expectedName);
        StringSubject subject = assertThat(data.text());
        if (expectedValue.equals("NULL")) {
            subject.isNull();
        } else {
            subject.isEqualTo(expectedValue);
        }
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
                         "data": [1, 2, 3],
                         "map": {"a": 1, "b": 2, "c": 3}
                    }
                        """);

        Map<String, String> target = new HashMap<>();
        KafkaRecord<?, JsonNode> record = fromValue(message);

        valueSelector(Expression("VALUE")).extractValueInto(record, target);
        assertThat(target)
                .containsExactly(
                        "name",
                        "joe",
                        "signature",
                        "YWJjZA",
                        "data",
                        "[1,2,3]",
                        "map",
                        "{\"a\":1,\"b\":2,\"c\":3}");
        target.clear();

        valueSelector(Expression("VALUE.name")).extractValueInto(record, target);
        assertThat(target).containsExactly("name", "joe");
        target.clear();

        valueSelector(Expression("VALUE.data")).extractValueInto(record, target);
        assertThat(target).containsExactly("data[0]", "1", "data[1]", "2", "data[2]", "3");
        target.clear();

        valueSelector(Expression("VALUE.data[1]")).extractValueInto(record, target);
        assertThat(target).containsExactly("data[1]", "2");
        target.clear();

        valueSelector(Expression("VALUE.data[2]")).extractValueInto(record, target);
        assertThat(target).containsExactly("data[2]", "3");
        target.clear();

        valueSelector(Expression("VALUE.map")).extractValueInto(record, target);
        assertThat(target).containsExactly("a", "1", "b", "2", "c", "3");
        target.clear();

        valueSelector(Expression("VALUE.map.a")).extractValueInto(record, target);
        assertThat(target).containsExactly("a", "1");
        target.clear();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                  EXPECTED_ERROR_MESSAGE
                VALUE,                       The expression [VALUE] must evaluate to a non-complex object
                VALUE.no_attrib,             Field [no_attrib] not found
                VALUE['no_attrib'],          Field [no_attrib] not found
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
                VALUE.nullArray[0],          Cannot retrieve index [0] from a null object
                    """)
    public void shouldNotExtractValue(String expressionStr, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValue("param", fromValue(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
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
            textBlock =
                    """
                EXPRESSION,                  EXPECTED_ERROR_MESSAGE
                VALUE.no_attrib,             Field [no_attrib] not found
                VALUE['no_attrib'],          Field [no_attrib] not found
                VALUE.children[0].no_attrib, Field [no_attrib] not found
                VALUE.no_children[0],        Field [no_children] not found
                VALUE.name[0],               Field [name] is not indexed
                VALUE.name['no_key'],        Cannot retrieve field [no_key] from a scalar object
                VALUE.name.no_key,           Cannot retrieve field [no_key] from a scalar object
                VALUE.children[0]['no_key'], Field [no_key] not found
                VALUE.children[3].name,      Cannot retrieve field [name] from a null object
                VALUE.children[4],           Field not found at index [4]
                VALUE.children[4].name,      Field not found at index [4]
                VALUE.nullArray[0],          Cannot retrieve index [0] from a null object
                    """)
    public void shouldNotExtractValueIntoMap(String expressionStr, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValueInto(
                                                fromValue(SAMPLE_MESSAGE), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION             | EXPECTED_NAME | EXPECTED
                VALUE                  | VALUE         | {"root":{"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2}}}
                VALUE.root             | root          | {"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2}}
                VALUE.root.name        | name          | joe
                VALUE.root.emptyArray  | emptyArray    | []
                VALUE.root.array       | array         | [1,2,3]
                VALUE.root.emptyObject | emptyObject   | {}
                VALUE.root.object      | object        | {"a":1,"b":2}
                    """)
    public void shouldExtractValueWithNonScalars(
            String expressionString, String expectedName, String expected)
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

        Data autoBoundValue =
                valueSelector(Expression(expressionString)).extractValue(fromValue(message), false);
        assertThat(autoBoundValue.name()).isEqualTo(expectedName);
        assertThat(autoBoundValue.text()).isEqualTo(expected);

        Data boundValue =
                valueSelector(Expression(expressionString))
                        .extractValue("param", fromValue(message), false);
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                  EXPECTED_ERROR_MESSAGE
                VALUE,                       Cannot retrieve field [VALUE] from a null object
                VALUE.no_attrib,             Cannot retrieve field [VALUE] from a null object
                VALUE.children[0].no_attrib, Cannot retrieve field [VALUE] from a null object
                VALUE.no_children[0],        Cannot retrieve field [VALUE] from a null object
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

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValue("param", fromValue((JsonNode) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValueInto(
                                                fromValue((JsonNode) null), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                          BOUND,          EXPECTED
                KEY.name,                            name,           joe
                KEY['name'],                         name,           joe
                KEY.signature,                       signature,      YWJjZA==
                KEY.notes[0],                        note0,          note1
                KEY.children[0].name,                childName,      alex
                KEY.children[0]['name'],             childName,      alex
                KEY.children[0].signature,           childSignature, NULL
                KEY.children[1].name,                childName,      anna
                KEY.children[2].name,                childName,      serena
                KEY.children[3],                     child,          NULL
                KEY.children[1].children[0].name,    childName,      gloria
                KEY.children[1].children[1].name,    childName,      terence
                KEY.children[1].children[1]['name'], childName,      terence
                KEY.family[0][0].name,               familyMember,   bro00
                KEY.family[0][1].name,               familyMember,   bro01
                KEY.family[1][0].name,               familyMember,   bro10
                KEY.family[1][1].name,               familyMember,   bro11
                    """)
    public void shouldExtractBoundKey(String expressionStr, String param, String expected)
            throws ExtractionException {
        KeySelector<JsonNode> keySelector = keySelector(Expression(expressionStr));
        Data data = keySelector.extractKey(param, fromKey(SAMPLE_MESSAGE));
        assertThat(data.name()).isEqualTo(param);
        StringSubject subject = assertThat(data.text());
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
                EXPRESSION,                          EXPECTED_NAME, EXPECTED_VALUE
                KEY.name,                            name,          joe
                KEY['name'],                         name,          joe
                KEY.signature,                       signature,     YWJjZA==
                KEY.notes[0],                        notes[0],      note1
                KEY.children[0].name,                name,          alex
                KEY.children[0]['name'],             name,          alex
                KEY.children[0].signature,           signature,     NULL
                KEY.children[1].name,                name,          anna
                KEY.children[2].name,                name,          serena
                KEY.children[3],                     children[3]    NULL
                KEY.children[1].children[0].name,    name,          gloria
                KEY.children[1].children[1].name,    name,          terence
                KEY.children[1].children[1]['name'], name,          terence
                KEY.family[0][0].name,               name,          bro00
                KEY.family[0][1].name,               name,          bro01
                KEY.family[1][0].name,               name,          bro10
                KEY.family[1][1].name,               name,          bro11
                    """)
    public void shouldExtractAutoBoundKey(
            String expressionStr, String expectedName, String expectedValue)
            throws ExtractionException {
        KeySelector<JsonNode> keySelector = keySelector(Expression(expressionStr));
        Data data = keySelector.extractKey(fromKey(SAMPLE_MESSAGE), false);
        assertThat(data.name()).isEqualTo(expectedName);
        StringSubject subject = assertThat(data.text());
        if (expectedValue.equals("NULL")) {
            subject.isNull();
        } else {
            subject.isEqualTo(expectedValue);
        }
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
                         "data": [1, 2, 3],
                         "map": {"a": 1, "b": 2, "c": 3}
                    }
                        """);

        Map<String, String> target = new HashMap<>();
        KafkaRecord<JsonNode, ?> record = fromKey(message);

        keySelector(Expression("KEY")).extractKeyInto(record, target);
        assertThat(target)
                .containsExactly(
                        "name",
                        "joe",
                        "signature",
                        "YWJjZA",
                        "data",
                        "[1,2,3]",
                        "map",
                        "{\"a\":1,\"b\":2,\"c\":3}");
        target.clear();

        keySelector(Expression("KEY.name")).extractKeyInto(record, target);
        assertThat(target).containsExactly("name", "joe");
        target.clear();

        keySelector(Expression("KEY.data")).extractKeyInto(record, target);
        assertThat(target).containsExactly("data[0]", "1", "data[1]", "2", "data[2]", "3");
        target.clear();

        keySelector(Expression("KEY.data[1]")).extractKeyInto(record, target);
        assertThat(target).containsExactly("data[1]", "2");
        target.clear();

        keySelector(Expression("KEY.data[2]")).extractKeyInto(record, target);
        assertThat(target).containsExactly("data[2]", "3");
        target.clear();

        keySelector(Expression("KEY.map")).extractKeyInto(record, target);
        assertThat(target).containsExactly("a", "1", "b", "2", "c", "3");
        target.clear();

        keySelector(Expression("KEY.map.a")).extractKeyInto(record, target);
        assertThat(target).containsExactly("a", "1");
        target.clear();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                EXPECTED_ERROR_MESSAGE
                KEY,                       The expression [KEY] must evaluate to a non-complex object
                KEY.no_attrib,             Field [no_attrib] not found
                KEY['no_attrib'],         Field [no_attrib] not found
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
                KEY.nullArray[0],          Cannot retrieve index [0] from a null object
                    """)
    public void shouldNotExtractKey(String expressionStr, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKey("param", fromKey(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
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
            textBlock =
                    """
                EXPRESSION,                EXPECTED_ERROR_MESSAGE
                KEY.no_attrib,             Field [no_attrib] not found
                KEY['no_attrib'],         Field [no_attrib] not found
                KEY.children[0].no_attrib, Field [no_attrib] not found
                KEY.no_children[0],        Field [no_children] not found
                KEY.name[0],               Field [name] is not indexed
                KEY.name['no_key'],        Cannot retrieve field [no_key] from a scalar object
                KEY.name.no_key,           Cannot retrieve field [no_key] from a scalar object
                KEY.children[0]['no_key'], Field [no_key] not found
                KEY.children[3].name,      Cannot retrieve field [name] from a null object
                KEY.children[4],           Field not found at index [4]
                KEY.children[4].name,      Field not found at index [4]
                KEY.nullArray[0],          Cannot retrieve index [0] from a null object
                    """)
    public void shouldNotExtractKeyIntoMap(String expressionStr, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKeyInto(fromKey(SAMPLE_MESSAGE), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION           | EXPECTED_NAME | EXPECTED
                KEY                  | KEY           | {"root":{"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2}}}
                KEY.root             | root          | {"name":"joe","signature":"YWJjZA","emptyArray":[],"array":[1,2,3],"emptyObject":{},"object":{"a":1,"b":2}}
                KEY.root.name        | name          | joe
                KEY.root.emptyArray  | emptyArray    | []
                KEY.root.array       | array         | [1,2,3]
                KEY.root.emptyObject | emptyObject   | {}
                KEY.root.object      | object        | {"a":1,"b":2}
                    """)
    public void shouldExtractKeyWithNonScalars(
            String expressionString, String expectedName, String expected)
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

        Data autoBoundValue =
                keySelector(Expression(expressionString)).extractKey(fromKey(message), false);
        assertThat(autoBoundValue.name()).isEqualTo(expectedName);
        assertThat(autoBoundValue.text()).isEqualTo(expected);

        Data boundValue =
                keySelector(Expression(expressionString))
                        .extractKey("param", fromKey(message), false);
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                EXPECTED_ERROR_MESSAGE
                KEY,                       Cannot retrieve field [KEY] from a null object
                KEY.no_attrib,             Cannot retrieve field [KEY] from a null object
                KEY.children[0].no_attrib, Cannot retrieve field [KEY] from a null object
                KEY.no_children[0],        Cannot retrieve field [KEY] from a null object
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

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKey("param", fromKey((JsonNode) null))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKeyInto(fromKey((JsonNode) null), new HashMap<>()));
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }
}
