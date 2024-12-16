
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
import static com.lightstreamer.kafka.test_utils.Records.fromKey;
import static com.lightstreamer.kafka.test_utils.Records.fromValue;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.truth.StringSubject;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class KvpNodeSelectorsSuppliersTest {

    private String TEXT =
            "QCHARTTOT=2032;TRow=12790;QV=9;PV=43;TMSTMP=2024-04-3013:23:07;QCHART=1;VTOT=81316;QTOT=2032;O=30/04/2024-13:23:07;QA=9012;Q=1;PA=40;PCHART=43;NTRAD=106;UNDEFINED";

    static ValueSelector<String> valueSelector(ExtractionExpression expression)
            throws ExtractionException {
        return new KvpSelectorsSuppliers()
                .makeValueSelectorSupplier()
                .newSelector("name", expression);
    }

    static KeySelector<String> keySelector(ExtractionExpression expression)
            throws ExtractionException {
        return new KvpSelectorsSuppliers()
                .makeKeySelectorSupplier()
                .newSelector("name", expression);
    }

    @Test
    public void shouldMakeKeySelectorSupplierWith() {
        KvpSelectorsSuppliers s = new KvpSelectorsSuppliers();
        KeySelectorSupplier<String> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.deseralizer().getClass())
                .isEqualTo(StringDeserializer.class);
    }

    @Test
    public void shouldMakeValueSelectorSupplierWith() {
        KvpSelectorsSuppliers s = new KvpSelectorsSuppliers();
        ValueSelectorSupplier<String> keySelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(keySelectorSupplier.deseralizer().getClass())
                .isEqualTo(StringDeserializer.class);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                             EXPECTED
                        VALUE.QCHARTTOT,                        2032
                        VALUE.TRow,                             12790
                        VALUE.QV,                               9
                        VALUE.PV,                               43
                        VALUE.TMSTMP,                           2024-04-3013:23:07
                        VALUE.QCHART,                           1
                        VALUE.VTOT,                             81316
                        VALUE.QTOT,                             2032
                        VALUE.O,                                30/04/2024-13:23:07
                        VALUE.QA,                               9012
                        VALUE.Q,                                1
                        VALUE.PA,                               40
                        VALUE.PCHART,                           43
                        VALUE.NTRAD,                            106
                        VALUE.UNDEFINED,                        NULL
                        """)
    public void shouldExtractValue(String expressionStr, String expected)
            throws ExtractionException {
        String text =
                "QCHARTTOT=2032;TRow=12790;QV=9;PV=43;TMSTMP=2024-04-3013:23:07;QCHART=1;VTOT=81316;QTOT=2032;O=30/04/2024-13:23:07;QA=9012;Q=1;PA=40;PCHART=43;NTRAD=106;UNDEFINED";
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        StringSubject subject =
                assertThat(valueSelector(expression).extractValue(fromValue(text)).text());
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
                        VALUE.no_attrib,              Field [no_attrib] not found
                        VALUE.no_children[0],         Field [no_children] not found
                        VALUE.QCHARTTOT[0],           Field [QCHARTTOT] is not indexed
                        VALUE.QCHARTTOT['no_key'],    Field [no_key] not found
                        """)
    public void shouldNotExtractValue(String expressionStr, String errorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> valueSelector(expression).extractValue(fromValue(TEXT)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,                           EXPECTED
                        KEY.QCHARTTOT,                        2032
                        KEY.TRow,                             12790
                        KEY.QV,                               9
                        KEY.PV,                               43
                        KEY.TMSTMP,                           2024-04-3013:23:07
                        KEY.QCHART,                           1
                        KEY.VTOT,                             81316
                        KEY.QTOT,                             2032
                        KEY.O,                                30/04/2024-13:23:07
                        KEY.QA,                               9012
                        KEY.Q,                                1
                        KEY.PA,                               40
                        KEY.PCHART,                           43
                        KEY.NTRAD,                            106
                        KEY.UNDEFINED,                        NULL
                        """)
    public void shouldExtractKey(String expressionStr, String expected) throws ExtractionException {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        StringSubject subject =
                assertThat(keySelector(expression).extractKey(fromKey(TEXT)).text());
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
                        KEY.no_attrib,              Field [no_attrib] not found
                        KEY.no_children[0],         Field [no_children] not found
                        KEY.QCHARTTOT[0],           Field [QCHARTTOT] is not indexed
                        KEY.QCHARTTOT['no_key'],    Field [no_key] not found
                        """)
    public void shouldNotExtractKey(String expressionStr, String errorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> keySelector(expression).extractKey(fromKey(TEXT)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,          EXPECTED_ERROR_MESSAGE
                        VALUE,               Found the invalid expression [VALUE] with missing attribute while evaluating [name]
                        VALUE.a. .b,         Found the invalid expression [VALUE.a. .b] with missing tokens while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[0]xsd,  Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
                        VALUE.attrib[],      Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
                        VALUE.attrib[a],     Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
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
                        EXPRESSION,        EXPECTED_ERROR_MESSAGE
                        KEY,               Found the invalid expression [KEY] with missing attribute while evaluating [name]
                        KEY.a. .b,         Found the invalid expression [KEY.a. .b] with missing tokens while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[0]xsd,  Found the invalid indexed expression [KEY.attrib[0]xsd] while evaluating [name]
                        KEY.attrib[],      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
                        KEY.attrib[a],     Found the invalid indexed expression [KEY.attrib[a]] while evaluating [name]
                    """)
    public void shouldNotCreateKeySelector(String expressionStr, String expectedErrorMessage) {
        ExtractionExpression expression = Expressions.Expression(expressionStr);
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
