
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
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializers.GenericRecordLocalSchemaDeserializer;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;

public class GenericRecordSelectorsSuppliersTest {

    // A configuration with proper evaluator type settings for key and value
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

    static KeySelector<GenericRecord> keySelector(String expression) throws ExtractionException {
        return new GenericRecordSelectorsSuppliers(CONFIG)
                .makeKeySelectorSupplier()
                .newSelector(Expression(expression));
    }

    static ValueSelector<GenericRecord> valueSelector(String expression)
            throws ExtractionException {
        return new GenericRecordSelectorsSuppliers(CONFIG)
                .makeValueSelectorSupplier()
                .newSelector(Expression(expression));
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
        KeySelectorSupplier<GenericRecord> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.evaluatorType()).isEqualTo(EvaluatorType.AVRO);
    }

    @Test
    public void shouldNotMakeKeySelectorSupplierDueToMissingEvaluatorType() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        GenericRecordSelectorsSuppliers s = new GenericRecordSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeKeySelectorSupplier());
        assertThat(ie).hasMessageThat().isEqualTo("Evaluator type is not AVRO");
    }

    @Test
    public void shouldMakeKeySelector() throws ExtractionException {
        KeySelector<GenericRecord> selector = keySelector("KEY");

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
    public void shouldNotNotMakeKeySelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> keySelector(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
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
        ValueSelectorSupplier<GenericRecord> valueSelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(valueSelectorSupplier.evaluatorType()).isEqualTo(EvaluatorType.AVRO);
    }

    @Test
    public void shouldNotMakeValueSelectorSupplierDueToMissingEvaluatorType() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        GenericRecordSelectorsSuppliers s = new GenericRecordSelectorsSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeValueSelectorSupplier());
        assertThat(ie).hasMessageThat().isEqualTo("Evaluator type is not AVRO");
    }

    @Test
    public void shouldMakeValueSelector() throws ExtractionException {
        ValueSelector<GenericRecord> selector = valueSelector("VALUE");
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
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> valueSelector(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
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
                EXPRESSION                            | EXPECTED_NAME | EXPECTED_VALUE
                VALUE.name                            | name          | joe
                VALUE.preferences['pref1']            | pref1         | pref_value1
                VALUE.preferences['pref2']            | pref2         | pref_value2
                VALUE.documents['id'].doc_id          | doc_id        | ID123
                VALUE.documents['id'].doc_type        | doc_type      | ID
                VALUE.type                            | type          | TYPE1
                VALUE.signature                       | signature     | [97, 98, 99, 100]
                VALUE.children[0].name                | name          | alex
                VALUE.children[0]['name']             | name          | alex
                VALUE.children[0].signature           | signature     |
                VALUE.children[1].name                | name          | anna
                VALUE.children[2].name                | name          | serena
                VALUE.children[3]                     | children[3]   |
                VALUE.children[1].children[0].name    | name          | gloria
                VALUE.children[1].children[1].name    | name          | terence
                VALUE.children[1].children[1]['name'] | name          | terence
                VALUE.nullValue                       | nullValue     |
                    """)
    public void shouldExtractValue(String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        ValueSelector<GenericRecord> valueSelector = valueSelector(expression);

        Data autoBoundData = valueSelector.extractValue(fromValue(SAMPLE_MESSAGE));
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundValue = valueSelector.extractValue("param", fromValue(SAMPLE_MESSAGE));
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldExtractValueIntoMap() throws ValueException, ExtractionException {
        Map<String, String> target = new HashMap<>();
        KafkaRecord<?, GenericRecord> record = fromValue(SAMPLE_MESSAGE);

        valueSelector("VALUE").extractValueInto(record, target);
        assertThat(target)
                .containsAtLeast(
                        "name",
                        "joe",
                        "preferences",
                        "{pref1=pref_value1, pref2=pref_value2}",
                        "documents",
                        "{id={\"doc_id\": \"ID123\", \"doc_type\": \"ID\"}}",
                        "type",
                        "TYPE1",
                        "signature",
                        "[97, 98, 99, 100]",
                        "emptyArray",
                        "[]",
                        "array",
                        "[abc, xyz, null]");
        target.clear();

        valueSelector("VALUE.preferences").extractValueInto(record, target);
        assertThat(target).containsExactly("pref1", "pref_value1", "pref2", "pref_value2");
        target.clear();

        valueSelector("VALUE.documents.id").extractValueInto(record, target);
        assertThat(target).containsExactly("doc_id", "ID123", "doc_type", "ID");
        target.clear();

        valueSelector("VALUE.children").extractValueInto(record, target);
        assertThat(target).containsKey("children[0]");
        assertThat(target).containsKey("children[1]");
        assertThat(target).containsKey("children[2]");
        target.clear();

        valueSelector("VALUE.array").extractValueInto(record, target);
        assertThat(target).containsExactly("array[0]", "abc", "array[1]", "xyz", "array[2]", null);
        target.clear();

        valueSelector("VALUE.nullValue").extractValueInto(record, target);
        assertThat(target).isEmpty();
        target.clear();

        valueSelector("VALUE.emptyArray").extractValueInto(record, target);
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
                VALUE.no_attrib             | Field [no_attrib] not found
                VALUE['no_attrib']          | Field [no_attrib] not found
                VALUE.children[0].no_attrib | Field [no_attrib] not found
                VALUE.no_children[0]        | Field [no_children] not found
                VALUE.name[0]               | Field [name] is not indexed
                VALUE.name['no_key']        | Cannot retrieve field [no_key] from a scalar object
                VALUE.name.no_key           | Cannot retrieve field [no_key] from a scalar object
                VALUE.preferences           | The expression [VALUE.preferences] must evaluate to a non-complex object
                VALUE.documents             | The expression [VALUE.documents] must evaluate to a non-complex object
                VALUE.children              | The expression [VALUE.children] must evaluate to a non-complex object
                VALUE.children[0]['no_key'] | Field [no_key] not found
                VALUE.children[0]           | The expression [VALUE.children[0]] must evaluate to a non-complex object
                VALUE.children[3].name      | Cannot retrieve field [name] from a null object
                VALUE.children[4]           | Field not found at index [4]
                VALUE.children[4].name      | Field not found at index [4]
                VALUE.type.attrib           | Cannot retrieve field [attrib] from a scalar object
                VALUE.emptyArray[0]         | Field not found at index [0]
                VALUE.nullValue[0]          | Cannot retrieve index [0] from a null object
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
                VALUE.type.attrib           | Cannot retrieve field [attrib] from a scalar object
                VALUE.emptyArray[0]         | Field not found at index [0]
                VALUE.nullValue[0]          | Cannot retrieve index [0] from a null object
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
            delimiter = '|', // Required because of the expected value for input VALUE.signature
            textBlock =
                    """
                EXPRESSION                      | EXPECTED_NAME | EXPECTED
                VALUE                           | VALUE         | {"name": "joe", "type": "TYPE1", "signature": [97, 98, 99, 100], "main_document": null, "children": null, "emptyArray": [], "array": ["abc", "xyz", null], "nullValue": null, "preferences": {"pref1": "pref_value1", "pref2": "pref_value2"}, "documents": {"id": {"doc_id": "ID123", "doc_type": "ID"}}}
                VALUE.documents                 | documents     | {id: {"doc_id": "ID123", "doc_type": "ID"}}
                VALUE.documents.id.doc_id       | doc_id        | ID123
                VALUE.documents['id'].doc_id    | doc_id        | ID123
                VALUE.documents['id']['doc_id'] | doc_id        | ID123
                VALUE.preferences               | preferences   | {pref1: pref_value1, pref2: pref_value2}
                VALUE.preferences['pref1']      | pref1         | pref_value1
                VALUE.preferences['pref2']      | pref2         | pref_value2
                VALUE.type                      | type          | TYPE1
                VALUE.emptyArray                | emptyArray    | []
                VALUE.nullValue                 | nullValue     |
                    """)
    public void shouldExtractValueWithNonScalars(
            String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        ValueSelector<GenericRecord> valueSelector = valueSelector(expression);

        Data autoBoundValue = valueSelector.extractValue(fromValue(SAMPLE_MESSAGE_V2), false);
        assertThat(autoBoundValue.name()).isEqualTo(expectedName);
        assertThat(autoBoundValue.text()).isEqualTo(expectedValue);

        Data boundValue =
                valueSelector(expression)
                        .extractValue("param", fromValue(SAMPLE_MESSAGE_V2), false);
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                  | EXPECTED_ERROR_MESSAGE
                VALUE                       | Cannot retrieve field [VALUE] from a null object
                VALUE.no_attrib             | Cannot retrieve field [VALUE] from a null object
                VALUE.children[0].no_attrib | Cannot retrieve field [VALUE] from a null object
                VALUE.no_children[0]        | Cannot retrieve field [VALUE] from a null object
                    """)
    public void shouldHandleNullValue(String expression, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue(fromValue((GenericRecord) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue("param", fromValue((GenericRecord) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValueInto(
                                                fromValue((GenericRecord) null), new HashMap<>()));
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
                KEY.preferences['pref1']            | pref1         | pref_value1
                KEY.preferences['pref2']            | pref2         | pref_value2
                KEY.documents['id'].doc_id          | doc_id        | ID123
                KEY.documents['id'].doc_type        | doc_type      | ID
                KEY.type                            | type          | TYPE1
                KEY.signature                       | signature     | [97, 98, 99, 100]
                KEY.children[0].name                | name          | alex
                KEY.children[0]['name']             | name          | alex
                KEY.children[0].signature           | signature     |
                KEY.children[1].name                | name          | anna
                KEY.children[2].name                | name          | serena
                KEY.children[3]                     | children[3]   |
                KEY.children[1].children[0].name    | name          | gloria
                KEY.children[1].children[1].name    | name          | terence
                KEY.children[1].children[1]['name'] | name          | terence
                KEY.nullValue                       | nullValue     |
                    """)
    public void shouldExtractKey(String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        KeySelector<GenericRecord> keySelector = keySelector(expression);

        Data autoBoundData = keySelector.extractKey(fromKey(SAMPLE_MESSAGE));
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(expectedValue);

        Data boundData = keySelector.extractKey("param", fromKey(SAMPLE_MESSAGE));
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldExtractKeyIntoMap() throws ValueException, ExtractionException {
        Map<String, String> target = new HashMap<>();
        KafkaRecord<GenericRecord, ?> record = fromKey(SAMPLE_MESSAGE);

        keySelector("KEY").extractKeyInto(record, target);
        assertThat(target)
                .containsAtLeast(
                        "name",
                        "joe",
                        "preferences",
                        "{pref1=pref_value1, pref2=pref_value2}",
                        "documents",
                        "{id={\"doc_id\": \"ID123\", \"doc_type\": \"ID\"}}",
                        "type",
                        "TYPE1",
                        "signature",
                        "[97, 98, 99, 100]",
                        "emptyArray",
                        "[]",
                        "array",
                        "[abc, xyz, null]");
        target.clear();

        keySelector("KEY.preferences").extractKeyInto(record, target);
        assertThat(target).containsExactly("pref1", "pref_value1", "pref2", "pref_value2");
        target.clear();

        keySelector("KEY.documents.id").extractKeyInto(record, target);
        assertThat(target).containsExactly("doc_id", "ID123", "doc_type", "ID");
        target.clear();

        keySelector("KEY.children").extractKeyInto(record, target);
        assertThat(target).containsKey("children[0]");
        assertThat(target).containsKey("children[1]");
        assertThat(target).containsKey("children[2]");
        target.clear();

        keySelector("KEY.array").extractKeyInto(record, target);
        assertThat(target).containsExactly("array[0]", "abc", "array[1]", "xyz", "array[2]", null);
        target.clear();

        keySelector("KEY.nullValue").extractKeyInto(record, target);
        assertThat(target).isEmpty();
        target.clear();

        keySelector("KEY.emptyArray").extractKeyInto(record, target);
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
                KEY.no_attrib             | Field [no_attrib] not found
                KEY.children[0].no_attrib | Field [no_attrib] not found
                KEY.no_children[0]        | Field [no_children] not found
                KEY.name[0]               | Field [name] is not indexed
                KEY.name['no_key']        | Cannot retrieve field [no_key] from a scalar object
                KEY.name.no_key           | Cannot retrieve field [no_key] from a scalar object
                KEY.preferences           | The expression [KEY.preferences] must evaluate to a non-complex object
                KEY.children              | The expression [KEY.children] must evaluate to a non-complex object
                KEY.children[0]['no_key'] | Field [no_key] not found
                KEY.children[0]           | The expression [KEY.children[0]] must evaluate to a non-complex object
                KEY.children[3].name      | Cannot retrieve field [name] from a null object
                KEY.children[4]           | Field not found at index [4]
                KEY.children[4].name      | Field not found at index [4]
                KEY.type.attrib           | Cannot retrieve field [attrib] from a scalar object
                KEY.nullValue[0]          | Cannot retrieve index [0] from a null object
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
                EXPRESSION                 | EXPECTED_ERROR_MESSAGE
                KEY.no_attrib              | Field [no_attrib] not found
                KEY.children[0].no_attrib  | Field [no_attrib] not found
                KEY.no_children[0]         | Field [no_children] not found
                KEY.name[0]                | Field [name] is not indexed
                KEY.name['no_key']         | Cannot retrieve field [no_key] from a scalar object
                KEY.name.no_key            | Cannot retrieve field [no_key] from a scalar object
                KEY.children[0]['no_key']  | Field [no_key] not found
                KEY.children[3].name       | Cannot retrieve field [name] from a null object
                KEY.children[4]            | Field not found at index [4]
                KEY.children[4].name       | Field not found at index [4]
                KEY.type.attrib            | Cannot retrieve field [attrib] from a scalar object
                KEY.nullValue[0]           | Cannot retrieve index [0] from a null object
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
                EXPRESSION                    | EXPECTED_NAME | EXPECTED_VALUE
                KEY                           | KEY           | {"name": "joe", "type": "TYPE1", "signature": [97, 98, 99, 100], "main_document": null, "children": null, "emptyArray": [], "array": ["abc", "xyz", null], "nullValue": null, "preferences": {"pref1": "pref_value1", "pref2": "pref_value2"}, "documents": {"id": {"doc_id": "ID123", "doc_type": "ID"}}}
                KEY.documents                 | documents     | {id: {"doc_id": "ID123", "doc_type": "ID"}}
                KEY.documents.id.doc_id       | doc_id        | ID123
                KEY.documents['id'].doc_id    | doc_id        | ID123
                KEY.documents['id']['doc_id'] | doc_id        | ID123
                KEY.preferences               | preferences   | {pref1: pref_value1, pref2: pref_value2}
                KEY.preferences['pref1']      | pref1         | pref_value1
                KEY.preferences['pref2']      | pref2         | pref_value2
                KEY.type                      | type          | TYPE1
                KEY.emptyArray                | emptyArray    | []
                KEY.nullValue                 | nullValue     |
                    """)
    public void shouldExtractKeyWithNonScalars(
            String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        Data autoBoundValue = keySelector(expression).extractKey(fromKey(SAMPLE_MESSAGE_V2), false);
        assertThat(autoBoundValue.name()).isEqualTo(expectedName);
        assertThat(autoBoundValue.text()).isEqualTo(expectedValue);

        Data boundValue =
                keySelector(expression).extractKey("param", fromKey(SAMPLE_MESSAGE_V2), false);
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                | EXPECTED_ERROR_MESSAGE
                KEY                       | Cannot retrieve field [KEY] from a null object
                KEY.no_attrib             | Cannot retrieve field [KEY] from a null object
                KEY.children[0].no_attrib | Cannot retrieve field [KEY] from a null object
                KEY.no_children[0]        | Cannot retrieve field [KEY] from a null object
                    """)
    public void shouldHandleNullKey(String expression, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKey(fromKey((GenericRecord) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKey("param", fromKey((GenericRecord) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKeyInto(
                                                fromKey((GenericRecord) null), new HashMap<>()));
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }
}
