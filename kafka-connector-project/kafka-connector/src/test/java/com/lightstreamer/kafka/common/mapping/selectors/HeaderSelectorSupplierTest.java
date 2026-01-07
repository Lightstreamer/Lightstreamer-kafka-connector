
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedNoWildcardCheck;

import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeaderSelectorSupplierTest {

    private static final Headers SAMPLE_RECORD_HEADERS =
            new RecordHeaders()
                    .add("name", "joe".getBytes(StandardCharsets.UTF_8))
                    .add("signature", "YWJjZA==".getBytes(StandardCharsets.UTF_8))
                    .add("accountId", "12345".getBytes(StandardCharsets.UTF_8))
                    .add("accountId", "67890".getBytes(StandardCharsets.UTF_8))
                    .add("docType", "type1".getBytes(StandardCharsets.UTF_8))
                    .add("docType", "type2".getBytes(StandardCharsets.UTF_8));

    private static final org.apache.kafka.connect.header.Headers SAMPLE_CONNECT_HEADERS =
            new ConnectHeaders()
                    .addBytes("name", "joe".getBytes(StandardCharsets.UTF_8))
                    .addBytes("signature", "YWJjZA==".getBytes(StandardCharsets.UTF_8))
                    .addBytes("accountId", "12345".getBytes(StandardCharsets.UTF_8))
                    .addBytes("accountId", "67890".getBytes(StandardCharsets.UTF_8))
                    .addBytes("docType", "type1".getBytes(StandardCharsets.UTF_8))
                    .addBytes("docType", "type2".getBytes(StandardCharsets.UTF_8));

    private static final List<KafkaRecord<?, ?>> RECORDS =
            new ArrayList<>() {
                {
                    add(Records.recordWithHeaders("key", "value", SAMPLE_RECORD_HEADERS));
                    add(Records.sinkWithHeaders("key", "value", SAMPLE_CONNECT_HEADERS));
                }
            };

    static HeadersSelector headersSelector(String expression) throws ExtractionException {
        return new HeadersSelectorSupplier()
                .newSelector(WrappedNoWildcardCheck("#{" + expression + "}"));
    }

    @Test
    public void shouldMakeHeaderSelector() throws ExtractionException {
        HeadersSelector selector = headersSelector("HEADERS");
        assertThat(selector.expression().expression()).isEqualTo("HEADERS");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                EXPRESSION,           EXPECTED_ERROR_MESSAGE
                HEADERS.attrib[],     Found the invalid indexed expression [HEADERS.attrib[]]
                HEADERS.attrib[0]xsd, Found the invalid indexed expression [HEADERS.attrib[0]xsd]
                HEADERS.attrib[],     Found the invalid indexed expression [HEADERS.attrib[]]
                HEADERS.attrib[a],    Found the invalid indexed expression [HEADERS.attrib[a]]
                """)
    public void shouldNotMakeHeaderSelector(String expression, String expectedErrorMessage) {
        ExtractionException ee =
                assertThrows(ExtractionException.class, () -> headersSelector(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION              | EXPECTED_NAME | EXPECTED_VALUE
                HEADERS.name            | name          | joe
                HEADERS['name']         | name          | joe
                HEADERS[0]              | HEADERS[0]    | joe
                HEADERS.signature       | signature     | YWJjZA==
                HEADERS['signature']    | signature     | YWJjZA==
                HEADERS[1]              | HEADERS[1]    | YWJjZA==
                HEADERS.accountId[0]    | accountId[0]  | 12345
                HEADERS.accountId[1]    | accountId[1]  | 67890
                HEADERS['accountId'][0] | accountId[0]  | 12345
                HEADERS['accountId'][1] | accountId[1]  | 67890
                HEADERS[2]              | HEADERS[2]    | 12345
                HEADERS[3]              | HEADERS[3]    | 67890
                HEADERS.docType[0]      | docType[0]    | type1
                HEADERS.docType[1]      | docType[1]    | type2
                HEADERS['docType'][0]   | docType[0]    | type1
                HEADERS['docType'][1]   | docType[1]    | type2
                HEADERS[4]              | HEADERS[4]    | type1
                HEADERS[5]              | HEADERS[5]    | type2
                    """)
    public void shouldExtractHeaders(String expression, String expectedName, String expectedValue)
            throws ExtractionException {

        for (KafkaRecord<?, ?> record : RECORDS) {
            HeadersSelector headersSelector = headersSelector(expression);

            Data autoBoundData = headersSelector.extract(record);
            assertThat(autoBoundData.name()).isEqualTo(expectedName);
            assertThat(autoBoundData.text()).isEqualTo(expectedValue);

            Data boundData = headersSelector.extract("param", record);
            assertThat(boundData.name()).isEqualTo("param");
            assertThat(boundData.text()).isEqualTo(expectedValue);
        }
    }

    @Test
    public void shouldExtractRecordHeadersIntoMap() throws ExtractionException {
        Map<String, String> target = new HashMap<>();

        for (KafkaRecord<?, ?> record : RECORDS) {
            headersSelector("HEADERS").extractInto(record, target);
            assertThat(target)
                    .containsExactly(
                            "name", "joe",
                            "signature", "YWJjZA==",
                            "accountId[0]", "12345",
                            "accountId[1]", "67890",
                            "docType[0]", "type1",
                            "docType[1]", "type2");
            target.clear();

            headersSelector("HEADERS.accountId").extractInto(record, target);
            assertThat(target)
                    .containsExactly(
                            "accountId[0]", "12345",
                            "accountId[1]", "67890");
            target.clear();

            headersSelector("HEADERS['accountId']").extractInto(record, target);
            assertThat(target)
                    .containsExactly(
                            "accountId[0]", "12345",
                            "accountId[1]", "67890");
            target.clear();

            headersSelector("HEADERS.docType").extractInto(record, target);
            assertThat(target)
                    .containsExactly(
                            "docType[0]", "type1",
                            "docType[1]", "type2");
            target.clear();

            headersSelector("HEADERS['docType']").extractInto(record, target);
            assertThat(target)
                    .containsExactly(
                            "docType[0]", "type1",
                            "docType[1]", "type2");
            target.clear();

            headersSelector("HEADERS.accountId[0]").extractInto(record, target);
            assertThat(target).isEmpty();
            target.clear();

            headersSelector("HEADERS.name").extractInto(record, target);
            assertThat(target).isEmpty();
            target.clear();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                   | EXPECTED_ERROR_MESSAGE
                HEADERS                      | The expression [HEADERS] must evaluate to a non-complex object
                HEADERS.*                    | The expression [HEADERS.*] must evaluate to a non-complex object
                HEADERS[6]                   | Field not found at index [6]
                HEADERS['no_attrib']         | Field [no_attrib] not found
                HEADERS.no_attrib            | Field [no_attrib] not found
                HEADERS.no_children[0]       | Field [no_children] not found
                HEADERS.name[0]              | Field [name] is not indexed
                HEADERS.name['no_key']       | Cannot retrieve field [no_key] from a scalar object
                HEADERS.accountId            | The expression [HEADERS.accountId] must evaluate to a non-complex object
                HEADERS.accountId[0].account | Cannot retrieve field [account] from a scalar object
                HEADERS.accountId[2]         | Field not found at index [2]
                HEADERS['accountId']         | The expression [HEADERS['accountId']] must evaluate to a non-complex object
                            """)
    public void shouldNotExtractRecordHeader(String expression, String errorMessage) {
        for (KafkaRecord<?, ?> record : RECORDS) {
            ValueException ve =
                    assertThrows(
                            ValueException.class,
                            () -> headersSelector(expression).extract("name", record).text());
            assertThat(ve).hasMessageThat().isEqualTo(errorMessage);

            ve =
                    assertThrows(
                            ValueException.class,
                            () -> headersSelector(expression).extract(record).text());
            assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION                   | EXPECTED_ERROR_MESSAGE
                HEADERS['no_attrib']         | Field [no_attrib] not found
                HEADERS.no_attrib            | Field [no_attrib] not found
                HEADERS.no_children[0]       | Field [no_children] not found
                HEADERS.name[0]              | Field [name] is not indexed
                HEADERS.name['no_key']       | Cannot retrieve field [no_key] from a scalar object
                HEADERS.accountId[0].account | Cannot retrieve field [account] from a scalar object
                            """)
    public void shouldNotExtractRecordHeadersIntoMap(String expression, String errorMessage) {
        for (KafkaRecord<?, ?> record : RECORDS) {
            ValueException ve =
                    assertThrows(
                            ValueException.class,
                            () -> headersSelector(expression).extractInto(record, new HashMap<>()));
            assertThat(ve).hasMessageThat().isEqualTo(errorMessage);
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                EXPRESSION        | EXPECTED_NAME | EXPECTED_VALUE
                HEADERS           | HEADERS       | {name=joe, signature=YWJjZA==, accountId=12345, accountId=67890, docType=type1, docType=type2}
                HEADERS.accountId | accountId     | [12345, 67890]
                HEADERS.docType   | docType       | [type1, type2]
                        """)
    public void shouldExtractRecordHeaderWithNonScalars(
            String expression, String expectedName, String expectedValue)
            throws ExtractionException {
        HeadersSelector headersSelector = headersSelector(expression);
        for (KafkaRecord<?, ?> record : RECORDS) {
            Data autoBoundData = headersSelector.extract(record, false);
            assertThat(autoBoundData.name()).isEqualTo(expectedName);
            assertThat(autoBoundData.text()).isEqualTo(expectedValue);

            Data boundData = headersSelector.extract("param", record, false);
            assertThat(boundData.name()).isEqualTo("param");
            assertThat(boundData.text()).isEqualTo(expectedValue);
        }
    }
}
