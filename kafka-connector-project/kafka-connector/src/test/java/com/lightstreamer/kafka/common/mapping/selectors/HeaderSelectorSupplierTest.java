
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

package com.lightstreamer.kafka.common.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.common.expressions.Expressions.Expression;

import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;

public class HeaderSelectorSupplierTest {

    private static final Headers SAMPLE_HEADERS;

    static {
        SAMPLE_HEADERS =
                new RecordHeaders()
                        .add("name", "joe".getBytes(StandardCharsets.UTF_8))
                        .add("signature", "YWJjZA==".getBytes(StandardCharsets.UTF_8))
                        .add("accountId", "12345".getBytes(StandardCharsets.UTF_8))
                        .add("accountId", "67890".getBytes(StandardCharsets.UTF_8));
    }

    static GenericSelector headersSelector(ExtractionExpression expression)
            throws ExtractionException {
        return new HeadersSelectorSupplier().newSelector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,           EXPECTED
                HEADERS.name,         joe
                HEADERS[0],           joe
                HEADERS['name'],      joe
                HEADERS.signature,    YWJjZA==
                HEADERS['signature'], YWJjZA==
                HEADERS.accountId[0], 12345
                HEADERS.accountId[1], 67890
                        """)
    public void shouldExtractHeaders(String expressionStr, String expected)
            throws ExtractionException {
        KafkaRecord<String, String> record =
                Records.recordWithHeaders("key", "value", SAMPLE_HEADERS);
        assertThat(headersSelector(Expression(expressionStr)).extract(record).text())
                .isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,                   EXPECTED_ERROR_MESSAGE
                HEADERS,                      The expression [HEADERS] must evaluate to a non-complex object
                HEADERS.no_attrib,            Field [no_attrib] not found
                HEADERS.no_children[0],       Field [no_children] not found
                HEADERS.name[0],              Field [name] is not indexed
                HEADERS.name['no_key'],       Cannot retrieve field [no_key] from a scalar object
                HEADERS.accountId,            The expression [HEADERS.accountId] must evaluate to a non-complex object
                HEADERS.accountId[0].account, Cannot retrieve field [account] from a scalar object
                HEADERS['accountId'],         The expression [HEADERS['accountId']] must evaluate to a non-complex object
                            """)
    public void shouldNotExtractValue(String expressionStr, String errorMessage) {
        KafkaRecord<String, String> record =
                Records.recordWithHeaders("key", "value", SAMPLE_HEADERS);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> headersSelector(Expression(expressionStr)).extract(record).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION        | EXPECTED
                HEADERS           | {name=joe, signature=YWJjZA==, accountId=12345, accountId=67890}
                HEADERS.accountId | [12345, 67890]
                        """)
    public void shouldExtractValueWithNonScalars(String expressionStr, String expected)
            throws ExtractionException {
        KafkaRecord<String, String> record =
                Records.recordWithHeaders("key", "value", SAMPLE_HEADERS);

        assertThat(headersSelector(Expression(expressionStr)).extract(record, false).text())
                .isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,           EXPECTED_ERROR_MESSAGE
                HEADERS.a. .b,        Found the invalid expression [HEADERS.a. .b] with missing tokens while evaluating [name]
                HEADERS.attrib[],     Found the invalid indexed expression [HEADERS.attrib[]] while evaluating [name]
                HEADERS.attrib[0]xsd, Found the invalid indexed expression [HEADERS.attrib[0]xsd] while evaluating [name]
                HEADERS.attrib[],     Found the invalid indexed expression [HEADERS.attrib[]] while evaluating [name]
                HEADERS.attrib[a],    Found the invalid indexed expression [HEADERS.attrib[a]] while evaluating [name]
                """)
    public void shouldNotCreateHeaderSelector(String expressionStr, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> headersSelector(Expression(expressionStr)));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
