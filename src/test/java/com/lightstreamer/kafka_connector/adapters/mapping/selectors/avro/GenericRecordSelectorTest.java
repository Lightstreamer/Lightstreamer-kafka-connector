
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

package com.lightstreamer.kafka_connector.adapters.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords.fromKey;
import static com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords.fromValue;
import static com.lightstreamer.kafka_connector.adapters.test_utils.GenericRecordProvider.RECORD;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueException;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Tag("unit")
public class GenericRecordSelectorTest {

    static ConnectorConfig config() {
        return ConnectorConfigProvider.minimalWith(
                Map.of(
                        ConnectorConfig.ADAPTER_DIR,
                        "src/test/resources",
                        ConnectorConfig.KEY_SCHEMA_FILE,
                        "value.avsc",
                        ConnectorConfig.VALUE_SCHEMA_FILE,
                        "value.avsc"));
    }

    static ValueSelector<GenericRecord> valueSelector(String expression) {
        return SelectorsSuppliers.avro(config())
                .valueSelectorSupplier()
                .newSelector("name", expression);
    }

    static KeySelector<GenericRecord> keySelector(String expression) {
        return GenericRecordSelectorsSuppliers.keySelectorSupplier(config())
                .newSelector("name", expression);
    }

    @Test
    public void shouldGetDeserializer() {
        Deserializer<GenericRecord> keyDeserializer =
                GenericRecordSelectorsSuppliers.keySelectorSupplier(config()).deseralizer();
        assertThat(keyDeserializer).isInstanceOf(GenericRecordDeserializer.class);
        assertThat(GenericRecordDeserializer.class.cast(keyDeserializer).isKey()).isTrue();

        Deserializer<GenericRecord> valueDeserializer =
                GenericRecordSelectorsSuppliers.valueSelectorSupplier(config()).deseralizer();
        assertThat(valueDeserializer).isInstanceOf(GenericRecordDeserializer.class);
        assertThat(GenericRecordDeserializer.class.cast(valueDeserializer).isKey()).isFalse();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
            EXPRESSION                             |  EXPECTED
            VALUE.name                             |  joe
            VALUE.preferences['pref1']             |  pref_value1
            VALUE.preferences['pref2']             |  pref_value2
            VALUE.documents['id'].doc_id           |  ID123
            VALUE.documents['id'].doc_type         |  ID
            VALUE.type                             |  TYPE1
            VALUE.signature                        |  [97, 98, 99, 100]
            VALUE.children[0].name                 |  alex
            VALUE.children[0]['name']              |  alex
            VALUE.children[0].signature            |  NULL
            VALUE.children[1].name                 |  anna
            VALUE.children[2].name                 |  serena
            VALUE.children[3]                      |  NULL
            VALUE.children[1].children[0].name     |  gloria
            VALUE.children[1].children[1].name     |  terence
            VALUE.children[1].children[1]['name']  |  terence
            """)
    public void shouldExtractValue(String expression, String expectedValue) {
        ValueSelector<GenericRecord> selector = valueSelector(expression);
        assertThat(selector.extract(fromValue(RECORD)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
            EXPRESSION,                         EXPECTED_ERROR_MESSAGE
            VALUE.no_attrib,                    Field [no_attrib] not found
            VALUE.children[0].no_attrib,        Field [no_attrib] not found
            VALUE.no_children[0],               Field [no_children] not found
            VALUE.name[0],                      Current field is not indexed
            VALUE.preferences,                  The expression [VALUE.preferences] must evaluate to a non-complex object
            VALUE.children,                     The expression [VALUE.children] must evaluate to a non-complex object
            VALUE.children[0]['no_key'],        Field [no_key] not found
            VALUE.children[0],                  The expression [VALUE.children[0]] must evaluate to a non-complex object
            VALUE.children[3].name,             Current fieldField not found at index [3]
            VALUE.children[4],                  Field not found at index [4]
            VALUE.children[4].name,             Field not found at index [4]
            VALUE.type.attrib,                  Current field [EnumSymbol] is a terminal object
            """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueSelector<GenericRecord> selector = valueSelector(expression);
        ValueException ve =
                assertThrows(
                        ValueException.class, () -> selector.extract(fromValue(RECORD)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
            EXPRESSION,                       EXPECTED
            KEY.name,                             joe
            KEY.children[0].name,                 alex
            KEY.children[0]['name'],              alex
            KEY.children[1].name,                 anna
            KEY.children[2].name,                 serena
            KEY.children[1].children[0].name,     gloria
            KEY.children[1].children[1].name,     terence
            KEY.children[1].children[1]['name'],  terence
            """)
    public void shouldExtractKey(String expression, String expectedValue) {
        KeySelector<GenericRecord> selector = keySelector(expression);
        assertThat(selector.extract(fromKey(RECORD)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
            EXPRESSION,                       EXPECTED_ERROR_MESSAGE
            KEY.no_attrib,                    Field [no_attrib] not found
            KEY.children[0].no_attrib,        Field [no_attrib] not found
            KEY.no_children[0],               Field [no_children] not found
            KEY.name[0],                      Current field is not indexed
            KEY.preferences,                  The expression [KEY.preferences] must evaluate to a non-complex object
            KEY.children,                     The expression [KEY.children] must evaluate to a non-complex object
            KEY.children[0]['no_key'],        Field [no_key] not found
            KEY.children[0],                  The expression [KEY.children[0]] must evaluate to a non-complex object
            KEY.children[3].name,             Field not found at index [3]
            """)
    public void shouldNotExtractKey(String expression, String errorMessage) {
        KeySelector<GenericRecord> selector = keySelector(expression);
        ValueException ve =
                assertThrows(ValueException.class, () -> selector.extract(fromKey(RECORD)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
            ESPRESSION,                        EXPECTED_ERROR_MESSAGE
            '',                                Expected the root token [KEY] while evaluating [name]
            invalidKey,                        Expected the root token [KEY] while evaluating [name]
            KEY,                               Found the invalid expression [KEY] while evaluating [name]
            KEY.,                              Found the invalid expression [KEY.] while evaluating [name]
            KEY..,                             Found the invalid expression [KEY..] with missing tokens while evaluating [name]
            KEY.attrib[],                      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
            KEY.attrib[0]xsd,                  Found the invalid indexed expression [KEY.attrib[0]xsd] while evaluating [name]
            KEY.attrib[],                      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
            KEY.attrib[a],                     Found the invalid indexed expression [KEY.attrib[a]] while evaluating [name]
            KEY.attrib[a].,                    Found the invalid indexed expression [KEY.attrib[a].] while evaluating [name]
            KEY.attrib[0].,                    Found the invalid indexed expression [KEY.attrib[0].] while evaluating [name]
            """)
    public void shouldNotCreateKeySelector(String expression, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
            ESPRESSION,                        EXPECTED_ERROR_MESSAGE
            '',                                Expected the root token [VALUE] while evaluating [name]
            invalidValue,                      Expected the root token [VALUE] while evaluating [name]
            VALUE,                             Found the invalid expression [VALUE] while evaluating [name]
            VALUE.,                            Found the invalid expression [VALUE.] while evaluating [name]
            VALUE..,                           Found the invalid expression [VALUE..] with missing tokens while evaluating [name]
            VALUE.attrib[],                    Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
            VALUE.attrib[0]xsd,                Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
            VALUE.attrib[],                    Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
            VALUE.attrib[a],                   Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
            VALUE.attrib[a].,                  Found the invalid indexed expression [VALUE.attrib[a].] while evaluating [name]
                """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
