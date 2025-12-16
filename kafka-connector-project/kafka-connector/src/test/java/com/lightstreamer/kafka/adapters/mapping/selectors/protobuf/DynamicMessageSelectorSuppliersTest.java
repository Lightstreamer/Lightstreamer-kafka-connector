
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
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.PROTOBUF;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedNoWildcardCheck;
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.InvalidEscapeSequenceException;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
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
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
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

    static KeySelector<DynamicMessage> keySelector(String expression) throws ExtractionException {
        return new DynamicMessageSelectorSuppliers(CONFIG)
                .makeKeySelectorSupplier()
                .newSelector(WrappedNoWildcardCheck("#{" + expression + "}"));
    }

    static ValueSelector<DynamicMessage> valueSelector(String expression)
            throws ExtractionException {
        return new DynamicMessageSelectorSuppliers(CONFIG)
                .makeValueSelectorSupplier()
                .newSelector(WrappedNoWildcardCheck("#{" + expression + "}"));
    }

    static String maybeEmpty(String expected) {
        return "<EMPTY>".equals(expected) ? "" : unescape(expected);
    }

    static String unescape(String expected) {
        try {
            return TextFormat.unescapeBytes(expected).toStringUtf8();
        } catch (InvalidEscapeSequenceException e) {
            throw new RuntimeException(e);
        }
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
        KeySelectorSupplier<DynamicMessage> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.evaluatorType()).isEqualTo(EvaluatorType.PROTOBUF);
    }

    @Test
    public void shouldNotMakeKeySelectorSupplierDueToMissingEvaluatorType() {
        // Configure the key evaluator type, but leave default settings for
        // RECORD_VALUE_EVALUATOR_TYPE (String)
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeKeySelectorSupplier());
        assertThat(ie).hasMessageThat().isEqualTo("Evaluator type is not PROTOBUF");
    }

    @Test
    public void shouldNotMakeKeySelectorSupplierDueToMissingMessageType() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");
        Path protoKeySchemaFile = adapterDir.resolve("person.proto.desc");

        // Configure the key evaluator type, but leave default settings for
        // RECORD_VALUE_EVALUATOR_TYPE (String)
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                "PROTOBUF",
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                protoKeySchemaFile.getFileName().toString(),
                                RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "InvalidMessageType"));
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeKeySelectorSupplier());
        assertThat(ie)
                .hasMessageThat()
                .isEqualTo(
                        "Message type [InvalidMessageType] not found in schema "
                                + protoKeySchemaFile.toAbsolutePath().toString());
    }

    @Test
    public void shouldMakeKeySelector() throws ExtractionException {
        KeySelector<DynamicMessage> selector = keySelector("KEY");
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
        ValueSelectorSupplier<DynamicMessage> valueSelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(valueSelectorSupplier.evaluatorType()).isEqualTo(EvaluatorType.PROTOBUF);
    }

    @Test
    public void shouldNotMakeValueSelectorSupplierDueToMissingEvaluatorType() {
        // Configure the value evaluator type, but leave default settings for
        // RECORD_VALUE_EVALUATOR_TYPE (String)
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeValueSelectorSupplier());
        assertThat(ie).hasMessageThat().isEqualTo("Evaluator type is not PROTOBUF");
    }

    @Test
    public void shouldNotMakeValueSelectorSupplierDueToMissingMessageType() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");
        Path protoValueSchemaFile = adapterDir.resolve("person.proto.desc");

        // Configure the value evaluator type, but leave default settings for
        // RECORD_KEY_EVALUATOR_TYPE (String)
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                "PROTOBUF",
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                protoValueSchemaFile.getFileName().toString(),
                                RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "valueMessage"));
        DynamicMessageSelectorSuppliers s = new DynamicMessageSelectorSuppliers(config);
        IllegalArgumentException ie =
                assertThrows(IllegalArgumentException.class, () -> s.makeValueSelectorSupplier());
        assertThat(ie)
                .hasMessageThat()
                .isEqualTo(
                        "Message type [valueMessage] not found in schema "
                                + protoValueSchemaFile.toAbsolutePath().toString());
    }

    @Test
    public void shouldMakeValueSelector() throws ExtractionException {
        ValueSelector<DynamicMessage> selector = valueSelector("VALUE");
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
        Deserializer<DynamicMessage> keyDeserializer =
                new DynamicMessageSelectorSuppliers(CONFIG)
                        .makeKeySelectorSupplier()
                        .deserializer();
        assertThat(keyDeserializer).isInstanceOf(KafkaProtobufDeserializer.class);

        Deserializer<DynamicMessage> valueDeserializer =
                new DynamicMessageSelectorSuppliers(CONFIG)
                        .makeValueSelectorSupplier()
                        .deserializer();
        assertThat(valueDeserializer).isInstanceOf(KafkaProtobufDeserializer.class);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                                | EXPECTED_NAME   | EXPECTED_VALUE
                VALUE.name                                | name            | joe
                VALUE['name']                             | name            | joe
                VALUE.email                               | email           | <EMPTY>
                VALUE['email']                            | email           | <EMPTY>
                VALUE.mainAddress.zip                     | zip             | <EMPTY>
                VALUE.mainAddress.country.name            | name            | <EMPTY>
                VALUE.otherAddresses['work'].city         | city            | Milan
                VALUE.otherAddresses['work'].country.name | name            | Italy
                VALUE.otherAddresses['club'].zip          | zip             | 96100
                VALUE.job                                 | job             | EMPLOYEE
                VALUE.signature                           | signature       | abcd
                VALUE.phoneNumbers[0]                     | phoneNumbers[0] | 012345
                VALUE.phoneNumbers[1]                     | phoneNumbers[1] | 123456
                VALUE.friends[0].name                     | name            | mike
                VALUE.friends[1]['name']                  | name            | john
                VALUE.simpleRoleName                      | simpleRoleName  | Software Architect
                VALUE.indexedAddresses['1'].city          | city            | Rome
                VALUE.booleanAddresses['true'].city       | city            | Turin
                VALUE.booleanAddresses['false'].city      | city            | Florence
                VALUE.any.type_url                        | type_url        | type.googleapis.com/Car
                VALUE.any.value                           | value           | \\n\\004FORD
                    """)
    public void shouldExtractValue(String expression, String expectedName, String expectedValue)
            throws ExtractionException, ValueException {
        ValueSelector<DynamicMessage> valueSelector = valueSelector(expression);

        Data autoBoundData = valueSelector.extractValue(fromValue(SAMPLE_MESSAGE));
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(maybeEmpty(expectedValue));

        Data boundData = valueSelector.extractValue("param", fromValue(SAMPLE_MESSAGE), true);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(maybeEmpty(expectedValue));
    }

    @Test
    public void shouldExtractValueIntoMap() throws ValueException, ExtractionException {
        Map<String, String> target = new HashMap<>();
        KafkaRecord<?, DynamicMessage> record = fromValue(SAMPLE_MESSAGE);

        valueSelector("VALUE.*").extractValueInto(record, target);
        assertThat(target)
                .containsAtLeast(
                        "name", "joe",
                        "job", "EMPLOYEE",
                        "simpleRoleName", "Software Architect",
                        "signature", "abcd");
        target.clear();

        valueSelector("VALUE.car").extractValueInto(record, target);
        assertThat(target).containsExactly("brand", "BMW");
        target.clear();

        valueSelector("VALUE.phoneNumbers").extractValueInto(record, target);
        assertThat(target)
                .containsExactly("phoneNumbers[0]", "012345", "phoneNumbers[1]", "123456");
        target.clear();

        valueSelector("VALUE.otherAddresses['club']").extractValueInto(record, target);
        assertThat(target)
                .containsExactly("zip", "96100", "city", "Siracusa", "street", "", "country", "");
        target.clear();

        valueSelector("VALUE.data").extractValueInto(record, target);
        assertThat(target).containsExactly("aDataKey", "-13.3");
        target.clear();

        valueSelector("VALUE.friends[0]").extractValueInto(record, target);
        assertThat(target).containsEntry("name", "mike");
        target.clear();

        valueSelector("VALUE.friends[1]").extractValueInto(record, target);
        assertThat(target).containsEntry("name", "john");
        target.clear();

        valueSelector("VALUE.friends[1].friends[0]").extractValueInto(record, target);
        assertThat(target).containsAtLeast("name", "robert", "signature", "abcd");
        target.clear();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                     | EXPECTED_ERROR_MESSAGE
                VALUE                          | The expression [VALUE] must evaluate to a non-complex object
                VALUE.no_attrib                | Field [no_attrib] not found
                VALUE['no_attrib']             | Field [no_attrib] not found
                VALUE.friends[0].no_attrib     | Field [no_attrib] not found
                VALUE.no_children[0]           | Field [no_children] not found
                VALUE.name[0]                  | Field [name] is not indexed
                VALUE.name['no_key']           | Cannot retrieve field [no_key] from a scalar object
                VALUE.name.no_key              | Cannot retrieve field [no_key] from a scalar object
                VALUE.phoneNumbers             | The expression [VALUE.phoneNumbers] must evaluate to a non-complex object
                VALUE.otherAddresses           | The expression [VALUE.otherAddresses] must evaluate to a non-complex object
                VALUE.otherAddresses['no_key'] | Field [no_key] not found
                VALUE.mainAddress              | The expression [VALUE.mainAddress] must evaluate to a non-complex object
                VALUE.friends[4]               | Field not found at index [4]
                VALUE.friends[4].name          | Field not found at index [4]
                VALUE.*                        | The expression [VALUE.*] must evaluate to a non-complex object
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
                EXPRESSION                     | EXPECTED_ERROR_MESSAGE
                VALUE.no_attrib                | Field [no_attrib] not found
                VALUE['no_attrib']             | Field [no_attrib] not found
                VALUE.friends[0].no_attrib     | Field [no_attrib] not found
                VALUE.no_children[0]           | Field [no_children] not found
                VALUE.name[0]                  | Field [name] is not indexed
                VALUE.name['no_key']           | Cannot retrieve field [no_key] from a scalar object
                VALUE.name.no_key              | Cannot retrieve field [no_key] from a scalar object
                VALUE.otherAddresses['no_key'] | Field [no_key] not found
                VALUE.friends[4]               | Field not found at index [4]
                VALUE.friends[4].name          | Field not found at index [4]
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
                EXPRESSION         | EXPECTED_NAME | EXPECTED_VALUE
                VALUE              | VALUE         | name: "mike"\\nmainAddress {\\n  city: "London"\\n  country {\\n    name: "England"\\n  }\\n}\\nphoneNumbers: "111111"\\nphoneNumbers: "222222"\\nfriends {\\n  name: "alex"\\n}\\njob: ARTIST\\n
                VALUE.phoneNumbers | phoneNumbers  | phoneNumbers: "111111"\\nphoneNumbers: "222222"\\n
                VALUE.friends      | friends       | friends {\\n  name: "alex"\\n}\\n
                VALUE.friends[0]   | friends[0]    | name: "alex"\\n
                VALUE.mainAddress  | mainAddress   | city: "London"\\ncountry {\\n  name: "England"\\n}\\n
                    """)
    public void shouldExtractValueWithNonScalars(
            String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        ValueSelector<DynamicMessage> valueSelector = valueSelector(expression);

        Data autoBoundData = valueSelector.extractValue(fromValue(SAMPLE_MESSAGE_V2), false);
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(unescape(expectedValue));

        Data boundData = valueSelector.extractValue("param", fromValue(SAMPLE_MESSAGE_V2), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(unescape(expectedValue));
    }

    @Test
    public void shouldHandleNullValue() throws ValueException, ExtractionException {
        Data autoBoundValue = valueSelector("VALUE").extractValue(fromValue((DynamicMessage) null));
        assertThat(autoBoundValue.name()).isEqualTo("VALUE");
        assertThat(autoBoundValue.text()).isNull();

        Data boundValue =
                valueSelector("VALUE").extractValue("param", fromValue((DynamicMessage) null));
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isNull();
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
                                        .extractValue(fromValue((DynamicMessage) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                valueSelector(expression)
                                        .extractValue("param", fromValue((DynamicMessage) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                              | EXPECTED_NAME   | EXPECTED_VALUE
                KEY.name                                | name            | joe
                KEY['name']                             | name            | joe
                KEY.email                               | email           | <EMPTY>
                KEY['email']                            | email           | <EMPTY>
                KEY.mainAddress.zip                     | zip             | <EMPTY>
                KEY.mainAddress.country.name            | name            | <EMPTY>
                KEY.otherAddresses['work'].city         | city            | Milan
                KEY.otherAddresses['work'].country.name | name            | Italy
                KEY.otherAddresses['club'].zip          | zip             | 96100
                KEY.job                                 | job             | EMPLOYEE
                KEY.signature                           | signature       | abcd
                KEY.phoneNumbers[0]                     | phoneNumbers[0] | 012345
                KEY.phoneNumbers[1]                     | phoneNumbers[1] | 123456
                KEY.friends[0].name                     | name            | mike
                KEY.friends[1]['name']                  | name            | john
                KEY.simpleRoleName                      | simpleRoleName  | Software Architect
                KEY.indexedAddresses['1'].city          | city            | Rome
                KEY.booleanAddresses['true'].city       | city            | Turin
                KEY.booleanAddresses['false'].city      | city            | Florence
                KEY.any.type_url                        | type_url        | type.googleapis.com/Car
                KEY.any.value                           | value           | \\n\\004FORD
                    """)
    public void shouldExtractKey(String expression, String expectedName, String expectedValue)
            throws ExtractionException, ValueException, InvalidEscapeSequenceException {
        KeySelector<DynamicMessage> keySelector = keySelector(expression);

        Data autoBoundData = keySelector.extractKey(fromKey(SAMPLE_MESSAGE));
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(maybeEmpty(expectedValue));

        Data boundData = keySelector.extractKey("param", fromKey(SAMPLE_MESSAGE), true);
        ;
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(maybeEmpty(expectedValue));
    }

    @Test
    public void shouldExtractKeyIntoMap() throws ValueException, ExtractionException {
        Map<String, String> target = new HashMap<>();
        KafkaRecord<DynamicMessage, ?> record = fromKey(SAMPLE_MESSAGE);

        keySelector("KEY.*").extractKeyInto(record, target);
        assertThat(target)
                .containsAtLeast(
                        "name", "joe",
                        "job", "EMPLOYEE",
                        "simpleRoleName", "Software Architect",
                        "signature", "abcd");
        target.clear();

        keySelector("KEY.car").extractKeyInto(record, target);
        assertThat(target).containsExactly("brand", "BMW");
        target.clear();

        keySelector("KEY.phoneNumbers").extractKeyInto(record, target);
        assertThat(target)
                .containsExactly("phoneNumbers[0]", "012345", "phoneNumbers[1]", "123456");
        target.clear();

        keySelector("KEY.otherAddresses['club']").extractKeyInto(record, target);
        assertThat(target)
                .containsExactly("zip", "96100", "city", "Siracusa", "street", "", "country", "");
        target.clear();

        keySelector("KEY.data").extractKeyInto(record, target);
        assertThat(target).containsExactly("aDataKey", "-13.3");
        target.clear();

        keySelector("KEY.friends[0]").extractKeyInto(record, target);
        assertThat(target).containsEntry("name", "mike");
        target.clear();

        keySelector("KEY.friends[1]").extractKeyInto(record, target);
        assertThat(target).containsEntry("name", "john");
        target.clear();

        keySelector("KEY.friends[1].friends[0]").extractKeyInto(record, target);
        assertThat(target).containsAtLeast("name", "robert", "signature", "abcd");
        target.clear();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                   | EXPECTED_ERROR_MESSAGE
                KEY                          | The expression [KEY] must evaluate to a non-complex object
                KEY.no_attrib                | Field [no_attrib] not found
                KEY.friends[0].no_attrib     | Field [no_attrib] not found
                KEY.no_children[0]           | Field [no_children] not found
                KEY.name[0]                  | Field [name] is not indexed
                KEY.name['no_key']           | Cannot retrieve field [no_key] from a scalar object
                KEY.name.no_key              | Cannot retrieve field [no_key] from a scalar object
                KEY.phoneNumbers             | The expression [KEY.phoneNumbers] must evaluate to a non-complex object
                KEY.otherAddresses           | The expression [KEY.otherAddresses] must evaluate to a non-complex object
                KEY.otherAddresses['no_key'] | Field [no_key] not found
                KEY.mainAddress              | The expression [KEY.mainAddress] must evaluate to a non-complex object
                KEY.friends[4]               | Field not found at index [4]
                KEY.friends[4].name          | Field not found at index [4]
                KEY.*                        | The expression [KEY.*] must evaluate to a non-complex object
                    """)
    public void shouldNotExtractKey(String expression, String errorMessage) {
        ValueException ve =
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
                EXPRESSION                   | EXPECTED_ERROR_MESSAGE
                KEY.no_attrib                | Field [no_attrib] not found
                KEY['no_attrib']             | Field [no_attrib] not found
                KEY.friends[0].no_attrib     | Field [no_attrib] not found
                KEY.no_children[0]           | Field [no_children] not found
                KEY.name[0]                  | Field [name] is not indexed
                KEY.name['no_key']           | Cannot retrieve field [no_key] from a scalar object
                KEY.name.no_key              | Cannot retrieve field [no_key] from a scalar object
                KEY.otherAddresses['no_key'] | Field [no_key] not found
                KEY.friends[4]               | Field not found at index [4]
                KEY.friends[4].name          | Field not found at index [4]
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
                EXPRESSION       | EXPECTED_NAME | EXPECTED_VALUE
                KEY              | KEY           | name: "mike"\\nmainAddress {\\n  city: "London"\\n  country {\\n    name: "England"\\n  }\\n}\\nphoneNumbers: "111111"\\nphoneNumbers: "222222"\\nfriends {\\n  name: "alex"\\n}\\njob: ARTIST\\n
                KEY.phoneNumbers | phoneNumbers  | phoneNumbers: "111111"\\nphoneNumbers: "222222"\\n
                KEY.friends      | friends       | friends {\\n  name: "alex"\\n}\\n
                KEY.friends[0]   | friends[0]    | name: "alex"\\n
                KEY.mainAddress  | mainAddress   | city: "London"\\ncountry {\\n  name: "England"\\n}\\n
                    """)
    public void shouldExtractKeyWithNonScalars(
            String expression, String expectedName, String expectedValue)
            throws ExtractionException, ValueException, InvalidEscapeSequenceException {
        KeySelector<DynamicMessage> keySelector = keySelector(expression);

        Data autoBoundData = keySelector.extractKey(fromKey(SAMPLE_MESSAGE_V2), false);
        assertThat(autoBoundData.name()).isEqualTo(expectedName);
        assertThat(autoBoundData.text()).isEqualTo(unescape(expectedValue));

        Data boundData = keySelector.extractKey("param", fromKey(SAMPLE_MESSAGE_V2), false);
        assertThat(boundData.name()).isEqualTo("param");
        assertThat(boundData.text()).isEqualTo(unescape(expectedValue));
    }

    @Test
    public void shouldHandleNullKey() throws ValueException, ExtractionException {
        Data autoBoundValue = keySelector("KEY").extractKey(fromKey((DynamicMessage) null));
        assertThat(autoBoundValue.name()).isEqualTo("KEY");
        assertThat(autoBoundValue.text()).isNull();

        Data boundValue = keySelector("KEY").extractKey("param", fromKey((DynamicMessage) null));
        assertThat(boundValue.name()).isEqualTo("param");
        assertThat(boundValue.text()).isNull();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                |  EXPECTED_ERROR_MESSAGE
                KEY.no_attrib              | Cannot retrieve field [no_attrib] from a null object
                KEY.children[0].no_attrib  | Cannot retrieve field [children] from a null object
                KEY.no_children[0]         | Cannot retrieve field [no_children] from a null object
                    """)
    public void shouldNotExtractFromNullKey(String expression, String errorMessage)
            throws ExtractionException {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKey(fromKey((DynamicMessage) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKey("param", fromKey((DynamicMessage) null))
                                        .text());
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

        ve =
                assertThrows(
                        ValueException.class,
                        () ->
                                keySelector(expression)
                                        .extractKeyInto(
                                                fromKey((DynamicMessage) null), new HashMap<>()));
        assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
    }
}
