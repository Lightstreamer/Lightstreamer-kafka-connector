
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

import com.lightstreamer.kafka.adapters.mapping.selectors.kvp.KvpSelectorsSuppliers.KvpNode;
import com.lightstreamer.kafka.common.utils.Split;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

public class KvpNodeTest {

    @ParameterizedTest
    @ValueSource(strings = {"key1=value1;key2=value2", "key1=value1;key2=value2;"})
    public void shouldParseValidString(String text) {
        KvpSelectorsSuppliers.KvpMap kvpMap = KvpSelectorsSuppliers.KvpMap.fromString(text);
        assertKvpMap(kvpMap);
    }

    @ParameterizedTest
    @ValueSource(strings = {"key1@value1|key2@value2", "key1@value1|key2@value2|"})
    public void shouldParseValidStringWithNonDefaultSeparators(String text) {
        KvpSelectorsSuppliers.KvpMap kvpMap =
                KvpSelectorsSuppliers.KvpMap.fromString(text, Split.on('|'), Split.on('@'));
        assertKvpMap(kvpMap);
    }

    private void assertKvpMap(KvpSelectorsSuppliers.KvpMap kvpMap) {
        assertThat(kvpMap.size()).isEqualTo(2);
        assertThat(kvpMap.isScalar()).isFalse();
        assertThat(kvpMap.isArray()).isFalse();
        assertThat(kvpMap.isNull()).isFalse();
        assertThat(kvpMap.has("key1")).isTrue();

        KvpNode key1Value = kvpMap.get("key1");
        assertThat(key1Value.size()).isEqualTo(0);
        assertThat(key1Value.isScalar()).isTrue();
        assertThat(key1Value.isArray()).isFalse();
        assertThat(key1Value.isNull()).isFalse();
        assertThat(key1Value.has("anyProp")).isFalse();
        assertThat(key1Value.get("anyProp").isNull()).isTrue();
        assertThat(key1Value.get(3).isNull()).isTrue();
        assertThat(key1Value.asText()).isEqualTo("value1");

        assertThat(kvpMap.has("key2")).isTrue();

        KvpNode key2Value = kvpMap.get("key2");
        assertThat(key2Value.size()).isEqualTo(0);
        assertThat(key2Value.isScalar()).isTrue();
        assertThat(key2Value.isArray()).isFalse();
        assertThat(key2Value.isNull()).isFalse();
        assertThat(key2Value.has("anyProp")).isFalse();
        assertThat(key2Value.get("anyProp").isNull()).isTrue();
        assertThat(key2Value.get(3).isNull()).isTrue();
        assertThat(key2Value.asText()).isEqualTo("value2");

        assertThat(kvpMap.has("key3")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"key1", "key1;"})
    public void shouldParseKeysOnly(String text) {
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString(text);
        assertThat(csvMap.size()).isEqualTo(1);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();

        KvpNode key1 = csvMap.get("key1");
        assertThat(key1.size()).isEqualTo(0);
        assertThat(key1.isScalar()).isTrue();
        assertThat(key1.isArray()).isFalse();
        assertThat(key1.isNull()).isFalse();
        assertThat(key1.asText()).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   ", "\t", "\n"})
    public void shouldHandleEmptyOrNullString(String text) {
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString(text);
        assertThat(csvMap.size()).isEqualTo(0);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isTrue();
        assertThat(csvMap.asText()).isEmpty();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                        INPUT                   | EXPECTED                   | EXPECTED_SIZE
                        key1                    | {key1=}                    | 1
                        key1;                   | {key1=}                    | 1
                        key1;key2               | {key1=, key2=}             | 2
                        key1=                   | {key1=}                    | 1
                        key1=value1             | {key1=value1}              | 1
                        key1=value1;            | {key1=value1}              | 1
                        key1=value1;key2=value2 | {key1=value1, key2=value2} | 2
                        key1=value1;key2        | {key1=value1, key2=}       | 2
                        key1=value1;key2;       | {key1=value1, key2=}       | 2
                        key1=value1;key2=       | {key1=value1, key2=}       | 2
                        key1=value1;=           | {key1=value1}              | 1
                        """)
    public void shouldReturnAsText(String input, String expected, int expectedSize) {
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString(input);
        assertThat(csvMap.size()).isEqualTo(expectedSize);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();
        assertThat(csvMap.asText()).isEqualTo(expected);
    }
}
