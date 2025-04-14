
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

package com.lightstreamer.kafka.adapters.mapping.selectors.protobuf;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.PROTOBUF;
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.truth.StringSubject;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

public class DynamicMessageSelectorSuppliersTest {

    static ConnectorConfig CONFIG =
            ConnectorConfigProvider.minimalWith(
                    Map.of(
                            SchemaRegistryConfigs.URL,
                            "https://localhost:8081",
                            RECORD_KEY_EVALUATOR_TYPE,
                            PROTOBUF.toString(),
                            RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                            "true",
                            RECORD_VALUE_EVALUATOR_TYPE,
                            PROTOBUF.toString(),
                            RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                            "true"));

    static DynamicMessage SAMPLE_MESSAGE =
            SampleMessageProviders.SampleDynamicMessageProvider().sampleMessage();
    static DynamicMessage SAMPLE_MESSAGE_V2 =
            SampleMessageProviders.SampleDynamicMessageProvider().sampleMessageV2();

    static ValueSelector<DynamicMessage> valueSelector(ExtractionExpression expression)
            throws ExtractionException {
        return new DynamicMessageSelectorSuppliers(CONFIG)
                .makeValueSelectorSupplier()
                .newSelector("name", expression);
    }

    static KeySelector<DynamicMessage> keySelector(ExtractionExpression expression)
            throws ExtractionException {
        return new DynamicMessageSelectorSuppliers(CONFIG)
                .makeKeySelectorSupplier()
                .newSelector("name", expression);
    }

    @Test
    public void shouldMakeKeySelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8081"));
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        assertDoesNotThrow(() -> s.makeKeySelectorSupplier());
    }

    @Test
    public void shouldNotMakeKeySelectorSupplierDueToMissingEvaluatorType() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeKeySelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not PROTOBUF");
    }

    @Test
    public void shouldMakeValueSelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                PROTOBUF.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                "true",
                                SchemaRegistryConfigs.URL,
                                "http://localhost:8081"));
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        assertDoesNotThrow(() -> s.makeValueSelectorSupplier());
    }

    @Test
    public void shouldNotMakeValueSelectorSupplierDueToMissingEvaluatorType() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeValueSelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not PROTOBUF");
    }

    @Test
    public void shouldGetDeserializer() {
        Deserializer<DynamicMessage> keyDeserializer =
                new DynamicMessageSelectorSuppliers(CONFIG)
                        .makeKeySelectorSupplier()
                        .deserializer();
        assertThat(keyDeserializer).isInstanceOf(KafkaProtobufDeserializer.class);
        // assertThat(DynamicMessageDeserializers.class.cast(keyDeserializer).isKey()).isTrue();

        Deserializer<DynamicMessage> valueDeserializer =
                new DynamicMessageSelectorSuppliers(CONFIG)
                        .makeValueSelectorSupplier()
                        .deserializer();
        assertThat(valueDeserializer).isInstanceOf(KafkaProtobufDeserializer.class);
        // assertThat(GenericRecordDeserializer.class.cast(valueDeserializer).isKey()).isFalse();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required because of the expected value for input VALUE.signature
            textBlock =
                    """
                        EXPRESSION                            |  EXPECTED
                        VALUE.name                            |  joe
                        VALUE.email                           | <EMPTY>
                        VALUE.mainAddress.zip                 | <EMPTY>
                        VALUE.otherAddresses['work'].city     | Milan
                        VALUE.otherAddresses['club'].zip      | 96100
                        VALUE.job                             |  EMPLOYEE
                        VALUE.signature                        |  abcd
                        VALUE.phoneNumbers[0]                | 012345
                        VALUE.phoneNumbers[1]                | 123456
                        VALUE.friends[0].name                |  mike
                        VALUE.friends[1]['name']             |  john
                        """)
    public void shouldExtractValue(String expressionStr, String expected)
            throws ExtractionException {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        String expectedStr = "<EMPTY>".equals(expected) ? "" : expected;
        assertThat(valueSelector(expression).extractValue(fromValue(SAMPLE_MESSAGE)).text())
                .isEqualTo(expectedStr);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required because of the expected value for input VALUE.signature
            textBlock =
                    """
                        EXPRESSION                      | EXPECTED
                        VALUE                           | {"name": "joe", "type": "TYPE1", "signature": [97, 98, 99, 100], "main_document": null, "children": null, "emptyArray": [], "nullArray": null, "preferences": {"pref1": "pref_value1", "pref2": "pref_value2"}, "documents": {"id": {"doc_id": "ID123", "doc_type": "ID"}}}
                        VALUE.documents                 | {id={"doc_id": "ID123", "doc_type": "ID"}}
                        VALUE.documents.id.doc_id       | ID123
                        VALUE.documents['id'].doc_id    | ID123
                        VALUE.documents['id']['doc_id'] | ID123
                        VALUE.preferences               | {pref1=pref_value1, pref2=pref_value2}
                        VALUE.preferences['pref1']      | pref_value1
                        VALUE.preferences['pref2']      | pref_value2
                        VALUE.type                      | TYPE1
                        VALUE.emptyArray                | []
                        """)
    public void shouldExtractValueWithNonScalars(String expressionStr, String expected)
            throws ExtractionException {
        ExtractionExpression expression = Expressions.Expression(expressionStr);

        String text =
                valueSelector(expression).extractValue(fromValue(SAMPLE_MESSAGE_V2), false).text();
        assertThat(text).isEqualTo(expected);
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
                        VALUE.preferences,           The expression [VALUE.preferences] must evaluate to a non-complex object
                        VALUE.documents,             The expression [VALUE.documents] must evaluate to a non-complex object
                        VALUE.children,              The expression [VALUE.children] must evaluate to a non-complex object
                        VALUE.children[0]['no_key'], Field [no_key] not found
                        VALUE.children[0],           The expression [VALUE.children[0]] must evaluate to a non-complex object
                        VALUE.children[3].name,      Cannot retrieve field [name] from a null object
                        VALUE.children[4],           Field not found at index [4]
                        VALUE.children[4].name,      Field not found at index [4]
                        VALUE.type.attrib,           Cannot retrieve field [attrib] from a scalar object
                        VALUE.emptyArray[0],         Field not found at index [0]
                        VALUE.nullArray[0],          Cannot retrieve index [0] from null object [nullArray]
                        """)
    public void shouldNotExtractValue(String expressionStr, String errorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue(fromValue(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required because of the expected value for input KEY.signature
            textBlock =
                    """
                        EXPRESSION                          | EXPECTED
                        KEY.name                            | joe
                        KEY.preferences['pref1']            | pref_value1
                        KEY.preferences['pref2']            | pref_value2
                        KEY.documents['id'].doc_id          | ID123
                        KEY.documents['id'].doc_type        | ID
                        KEY.type                            | TYPE1
                        KEY.signature                       | [97, 98, 99, 100]
                        KEY.children[0].name                | alex
                        KEY.children[0]['name']             | alex
                        KEY.children[0].signature           | NULL
                        KEY.children[1].name                | anna
                        KEY.children[2].name                | serena
                        KEY.children[3]                     | NULL
                        KEY.children[1].children[0].name    | gloria
                        KEY.children[1].children[1].name    | terence
                        KEY.children[1].children[1]['name'] | terence
                        KEY.nullArray                       | NULL
                        """)
    public void shouldExtractKey(String expressionStr, String expected) throws ExtractionException {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        StringSubject subject =
                assertThat(keySelector(expression).extractKey(fromKey(SAMPLE_MESSAGE)).text());
        if (expected.equals("NULL")) {
            subject.isNull();
        } else {
            subject.isEqualTo(expected);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|', // Required because of the expected value for input VALUE.signature
            textBlock =
                    """
                        EXPRESSION                    | EXPECTED
                        KEY                           | {"name": "joe", "type": "TYPE1", "signature": [97, 98, 99, 100], "main_document": null, "children": null, "emptyArray": [], "nullArray": null, "preferences": {"pref1": "pref_value1", "pref2": "pref_value2"}, "documents": {"id": {"doc_id": "ID123", "doc_type": "ID"}}}
                        KEY.documents                 | {id={"doc_id": "ID123", "doc_type": "ID"}}
                        KEY.documents.id.doc_id       | ID123
                        KEY.documents['id'].doc_id    | ID123
                        KEY.documents['id']['doc_id'] | ID123
                        KEY.preferences               | {pref1=pref_value1, pref2=pref_value2}
                        KEY.preferences['pref1']      | pref_value1
                        KEY.preferences['pref2']      | pref_value2
                        KEY.type                      | TYPE1
                        KEY.emptyArray                | []
                        """)
    public void shouldExtractKeyWithNonScalars(String expressionStr, String expected)
            throws ExtractionException {
        ExtractionExpression expression = Expressions.Expression(expressionStr);

        String text = keySelector(expression).extractKey(fromKey(SAMPLE_MESSAGE_V2), false).text();
        assertThat(text).isEqualTo(expected);
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
                        KEY.preferences,           The expression [KEY.preferences] must evaluate to a non-complex object
                        KEY.children,              The expression [KEY.children] must evaluate to a non-complex object
                        KEY.children[0]['no_key'], Field [no_key] not found
                        KEY.children[0],           The expression [KEY.children[0]] must evaluate to a non-complex object
                        KEY.children[3].name,      Cannot retrieve field [name] from a null object
                        KEY.children[4],           Field not found at index [4]
                        KEY.children[4].name,      Field not found at index [4]
                        KEY.type.attrib,           Cannot retrieve field [attrib] from a scalar object
                        KEY.nullArray[0],          Cannot retrieve index [0] from null object [nullArray]
                        """)
    public void shouldNotExtractKey(String expressionStr, String errorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> keySelector(expression).extractKey(fromKey(SAMPLE_MESSAGE)).text());
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
