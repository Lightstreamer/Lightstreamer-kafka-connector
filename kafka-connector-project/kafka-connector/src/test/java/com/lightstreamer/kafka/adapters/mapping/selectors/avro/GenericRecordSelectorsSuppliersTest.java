
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

package com.lightstreamer.kafka.adapters.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.AVRO;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecordFromKey;
import static com.lightstreamer.kafka.test_utils.Records.KafkaRecordFromValue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializers.GenericRecordLocalSchemaDeserializer;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

public class GenericRecordSelectorsSuppliersTest {

    static ConnectorConfig CONFIG =
            ConnectorConfigProvider.minimalWith(
                    "src/test/resources",
                    Map.of(
                            RECORD_KEY_EVALUATOR_TYPE,
                            AVRO.toString(),
                            RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                            "value.avsc",
                            RECORD_VALUE_EVALUATOR_TYPE,
                            AVRO.toString(),
                            RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                            "value.avsc"));

    static GenericRecord SAMPLE_MESSAGE =
            SampleMessageProviders.SampleGenericRecordProvider().sampleMessage();
    static GenericRecord SAMPLE_MESSAGE_V2 =
            SampleMessageProviders.SampleGenericRecordProvider().sampleMessageV2();

    static ValueSelector<GenericRecord> valueSelector(ExtractionExpression expression)
            throws ExtractionException {
        return new GenericRecordSelectorsSuppliers(CONFIG)
                .makeValueSelectorSupplier()
                .newSelector("name", expression);
    }

    static KeySelector<GenericRecord> keySelector(ExtractionExpression expression)
            throws ExtractionException {
        return new GenericRecordSelectorsSuppliers(CONFIG)
                .makeKeySelectorSupplier()
                .newSelector("name", expression);
    }

    @Test
    public void shouldMakeKeySelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        "src/test/resources",
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                "value.avsc"));
        GenericRecordSelectorsSuppliers s = new GenericRecordSelectorsSuppliers(config);
        assertDoesNotThrow(() -> s.makeKeySelectorSupplier());
    }

    @Test
    public void shouldNotMakeKeySelectorSupplierDueToMissingEvaluatorType() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        GenericRecordSelectorsSuppliers s = new GenericRecordSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeKeySelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not AVRO");
    }

    @Test
    public void shouldMakeValueSelectorSupplier() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        "src/test/resources",
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                AVRO.toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                "value.avsc"));
        GenericRecordSelectorsSuppliers s = new GenericRecordSelectorsSuppliers(config);
        assertDoesNotThrow(() -> s.makeValueSelectorSupplier());
    }

    @Test
    public void shouldNotMakeValueSelectorSupplierDueToMissingEvaluatorType() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        GenericRecordSelectorsSuppliers s = new GenericRecordSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeValueSelectorSupplier());
        assertThat(ie.getMessage()).isEqualTo("Evaluator type is not AVRO");
    }

    @Test
    public void shouldGetDeserializer() {
        Deserializer<GenericRecord> keyDeserializer =
                new GenericRecordSelectorsSuppliers(CONFIG)
                        .makeKeySelectorSupplier()
                        .deserializer();
        assertThat(keyDeserializer).isInstanceOf(GenericRecordLocalSchemaDeserializer.class);
        // assertThat(GenericRecordDeserializer.class.cast(keyDeserializer).isKey()).isTrue();

        Deserializer<GenericRecord> valueDeserializer =
                new GenericRecordSelectorsSuppliers(CONFIG)
                        .makeValueSelectorSupplier()
                        .deserializer();
        assertThat(valueDeserializer).isInstanceOf(GenericRecordLocalSchemaDeserializer.class);
        // assertThat(GenericRecordDeserializer.class.cast(valueDeserializer).isKey()).isFalse();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                            |  EXPECTED
                VALUE.name                            |  joe
                VALUE.preferences['pref1']            |  pref_value1
                VALUE.preferences['pref2']            |  pref_value2
                VALUE.documents['id'].doc_id          |  ID123
                VALUE.documents['id'].doc_type        |  ID
                VALUE.type                            |  TYPE1
                VALUE.signature                       |  [97, 98, 99, 100]
                VALUE.children[0].name                |  alex
                VALUE.children[0]['name']             |  alex
                VALUE.children[0].signature           |  NULL
                VALUE.children[1].name                |  anna
                VALUE.children[2].name                |  serena
                VALUE.children[3]                     |  NULL
                VALUE.children[1].children[0].name    |  gloria
                VALUE.children[1].children[1].name    |  terence
                VALUE.children[1].children[1]['name'] |  terence
                VALUE.nullArray                       |  NULL
                    """)
    public void shouldExtractValue(String expressionStr, String expected)
            throws ExtractionException {
        StringSubject subject =
                assertThat(
                        valueSelector(Expression(expressionStr))
                                .extractValue(KafkaRecordFromValue(SAMPLE_MESSAGE))
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
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(Expression(expressionStr))
                                        .extractValue(KafkaRecordFromValue(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
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
        String extractedData =
                valueSelector(Expression(expressionStr))
                        .extractValue(KafkaRecordFromValue(SAMPLE_MESSAGE_V2), false)
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
                                        .extractValue(KafkaRecordFromValue((GenericRecord) null))
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
        StringSubject subject =
                assertThat(
                        keySelector(Expression(expressionStr))
                                .extractKey(KafkaRecordFromKey(SAMPLE_MESSAGE))
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
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(Expression(expressionStr))
                                        .extractKey(KafkaRecordFromKey(SAMPLE_MESSAGE))
                                        .text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
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
        String extractedData =
                keySelector(Expression(expressionStr))
                        .extractKey(KafkaRecordFromKey(SAMPLE_MESSAGE_V2), false)
                        .text();
        assertThat(extractedData).isEqualTo(expected);
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
                                        .extractKey(KafkaRecordFromKey((GenericRecord) null))
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
