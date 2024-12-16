
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

public class KvpNodeTest {

    @Test
    public void shouldParseValidString() {
        String text = "key1=value1;key2=value2";
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString(text);

        assertThat(csvMap.size()).isEqualTo(2);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();
        assertThat(csvMap.has("key1")).isTrue();

        KvpNode key1 = csvMap.get("key1");
        assertThat(key1.size()).isEqualTo(0);
        assertThat(key1.isScalar()).isTrue();
        assertThat(key1.isArray()).isFalse();
        assertThat(key1.isNull()).isFalse();
        assertThat(key1.has("anyProp")).isFalse();
        assertThat(key1.get("anyProp")).isNull();
        assertThat(key1.get(3)).isNull();
        assertThat(key1.asText("")).isEqualTo("value1");

        assertThat(csvMap.has("key2")).isTrue();

        KvpNode key2 = csvMap.get("key2");
        assertThat(key1.size()).isEqualTo(0);
        assertThat(key2.isScalar()).isTrue();
        assertThat(key2.isArray()).isFalse();
        assertThat(key2.isNull()).isFalse();
        assertThat(key2.has("anyProp")).isFalse();
        assertThat(key2.get("anyProp")).isNull();
        assertThat(key2.get(3)).isNull();
        assertThat(key2.asText("")).isEqualTo("value2");

        assertThat(csvMap.has("key3")).isFalse();
    }

    @Test
    public void shouldParseStringWithNullValue() {
        String text = "key1";
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString(text);
        assertThat(csvMap.size()).isEqualTo(1);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();

        KvpNode key1 = csvMap.get("key1");
        assertThat(key1.size()).isEqualTo(0);
        assertThat(key1.isScalar()).isTrue();
        assertThat(key1.isArray()).isFalse();
        assertThat(key1.isNull()).isTrue();
        assertThat(key1.asText(null)).isNull();
        assertThat(key1.asText("NULL")).isEqualTo("NULL");
    }

    @Test
    public void shouldParseStringEndingWithNullValue() {
        String text = "key1;";
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString(text);
        assertThat(csvMap.size()).isEqualTo(2);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();

        KvpNode key1 = csvMap.get("key1");
        assertThat(key1.size()).isEqualTo(0);
        assertThat(key1.isScalar()).isTrue();
        assertThat(key1.isArray()).isFalse();
        assertThat(key1.isNull()).isTrue();
        assertThat(key1.asText(null)).isNull();
        assertThat(key1.asText("NULL")).isEqualTo("NULL");

        KvpNode nullKey = csvMap.get("");
        assertThat(nullKey.size()).isEqualTo(0);
        assertThat(nullKey.isScalar()).isTrue();
        assertThat(nullKey.isArray()).isFalse();
        assertThat(nullKey.isNull()).isTrue();
        assertThat(nullKey.asText(null)).isNull();
        assertThat(nullKey.asText("NULL")).isEqualTo("NULL");
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
        assertThat(csvMap.asText(null)).isNull();
        assertThat(csvMap.asText("NULL")).isEqualTo("NULL");
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                        INPUT                   | EXPECTED                        | EXPECTED_SIZE
                        key1                    | {key1=NULL_VALUE}               | 1
                        key1=value1             | {key1=value1}                   | 1
                        key1=value1;key2=value2 | {key1=value1, key2=value2}      | 2
                        key1=value1;key2        | {key1=value1, key2=NULL_VALUE}  | 2
                        key1=value1;            | {key1=value1, =NULL_VALUE}      | 2
                        """)
    public void shouldReturnAsText(String input, String expected, int expectedSize) {
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString(input);
        assertThat(csvMap.size()).isEqualTo(expectedSize);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();
        assertThat(csvMap.asText(null)).isEqualTo(expected);
    }
}
