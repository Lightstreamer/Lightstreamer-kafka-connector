
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
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.fromKey;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.fromValue;
import static com.lightstreamer.kafka.test_utils.JsonNodeProvider.RECORD;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class JsonNodeSelectorTest {

    static ConnectorConfig config = ConnectorConfigProvider.minimal();

    static ValueSelector<JsonNode> valueSelector(String expression) throws ExtractionException {
        return JsonNodeSelectorsSuppliers.valueSelectorSupplier(config)
                .newSelector("name", expression);
    }

    static KeySelector<JsonNode> keySelector(String expression) throws ExtractionException {
        return JsonNodeSelectorsSuppliers.keySelectorSupplier(config)
                .newSelector("name", expression);
    }

    @Test
    public void shouldGetDeserializer() {
        Deserializer<JsonNode> keyDeserializer =
                JsonNodeSelectorsSuppliers.keySelectorSupplier(config).deseralizer();
        assertThat(keyDeserializer).isInstanceOf(JsonNodeDeserializer.class);
        assertThat(JsonNodeDeserializer.class.cast(keyDeserializer).isKey()).isTrue();

        Deserializer<JsonNode> valueDeserializer =
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config).deseralizer();
        assertThat(valueDeserializer).isInstanceOf(JsonNodeDeserializer.class);
        assertThat(JsonNodeDeserializer.class.cast(valueDeserializer).isKey()).isFalse();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                             EXPECTED
                        VALUE.name,                             joe
                        VALUE.signature,                        YWJjZA==
                        VALUE.children[0].name,                 alex
                        VALUE.children[0]['name'],              alex
                        VALUE.children[0].signature,            NULL
                        VALUE.children[1].name,                 anna
                        VALUE.children[2].name,                 serena
                        VALUE.children[3],                      NULL
                        VALUE.children[1].children[0].name,     gloria
                        VALUE.children[1].children[1].name,     terence
                        VALUE.children[1].children[1]['name'],  terence
                        """)
    public void shouldExtractValue(String expression, String expected) throws ExtractionException {
        StringSubject subject =
                assertThat(valueSelector(expression).extract(fromValue(RECORD)).text());
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
                        EXPRESSION,                   EXPECTED_ERROR_MESSAGE
                        VALUE,                        The expression [VALUE] must evaluate to a non-complex object
                        VALUE.no_attrib,              Field [no_attrib] not found
                        VALUE.children[0].no_attrib,  Field [no_attrib] not found
                        VALUE.no_children[0],         Field [no_children] not found
                        VALUE.name[0],                Field [name] is not indexed
                        VALUE.name['no_key'],         Field [no_key] not found
                        VALUE.children,               The expression [VALUE.children] must evaluate to a non-complex object
                        VALUE.children[0]['no_key'],  Field [no_key] not found
                        VALUE.children[0],            The expression [VALUE.children[0]] must evaluate to a non-complex object
                        VALUE.children[3].name,       Cannot retrieve field [name] from a null object
                        VALUE.children[4],            Field not found at index [4]
                        VALUE.children[4].name,       Field not found at index [4]
                        """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> valueSelector(expression).extract(fromValue(RECORD)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                           EXPECTED
                        KEY.name,                             joe
                        KEY.signature,                        YWJjZA==
                        KEY.children[0].name,                 alex
                        KEY.children[0]['name'],              alex
                        KEY.children[0].signature,            NULL
                        KEY.children[1].name,                 anna
                        KEY.children[2].name,                 serena
                        KEY.children[3],                      NULL
                        KEY.children[1].children[0].name,     gloria
                        KEY.children[1].children[1].name,     terence
                        KEY.children[1].children[1]['name'],  terence
                        """)
    public void shouldExtractKey(String expression, String expected) throws ExtractionException {
        StringSubject subject = assertThat(keySelector(expression).extract(fromKey(RECORD)).text());
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
                        EXPRESSION,                 EXPECTED_ERROR_MESSAGE
                        KEY,                        The expression [KEY] must evaluate to a non-complex object
                        KEY.no_attrib,              Field [no_attrib] not found
                        KEY.children[0].no_attrib,  Field [no_attrib] not found
                        KEY.no_children[0],         Field [no_children] not found
                        KEY.name[0],                Field [name] is not indexed
                        KEY.children,               The expression [KEY.children] must evaluate to a non-complex object
                        KEY.children[0]['no_key'],  Field [no_key] not found
                        KEY.children[0],            The expression [KEY.children[0]] must evaluate to a non-complex object
                        KEY.children[3].name,       Cannot retrieve field [name] from a null object
                        KEY.children[4],            Field not found at index [4]
                        KEY.children[4].name,       Field not found at index [4]
                        """)
    public void shouldNotExtractKey(String expression, String errorMessage) {
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> keySelector(expression).extract(fromKey(RECORD)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,          EXPECTED_ERROR_MESSAGE
                        '',                  Expected the root token [VALUE] while evaluating [name]
                        invalidValue,        Expected the root token [VALUE] while evaluating [name]
                        VALUE..,             Found unexpected trailing dot(s) in the expression [VALUE..] while evaluating [name]
                        VALUE.a. .b,         Found the invalid expression [VALUE.a. .b] with missing tokens while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[0]xsd,  Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[a],     Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
                        VALUE.attrib[a].,    Found unexpected trailing dot(s) in the expression [VALUE.attrib[a].] while evaluating [name]
                    """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,        EXPECTED_ERROR_MESSAGE
                        '',                Expected the root token [KEY] while evaluating [name]
                        invalidKey,        Expected the root token [KEY] while evaluating [name]
                        KEY..,             Found unexpected trailing dot(s) in the expression [KEY..] while evaluating [name]
                        KEY.a. .b,         Found the invalid expression [KEY.a. .b] with missing tokens while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[0]xsd,  Found the invalid indexed expression [KEY.attrib[0]xsd] while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[a],     Found the invalid indexed expression [KEY.attrib[a]] while evaluating [name]
                        KEY.attrib[a].,    Found unexpected trailing dot(s) in the expression [KEY.attrib[a].] while evaluating [name]
                    """)
    public void shouldNotCreateKeySelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
