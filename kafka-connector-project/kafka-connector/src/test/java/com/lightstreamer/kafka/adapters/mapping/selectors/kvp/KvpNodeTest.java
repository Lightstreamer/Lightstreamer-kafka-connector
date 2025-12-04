
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
        KvpSelectorsSuppliers.KvpMap kvpMap = KvpSelectorsSuppliers.KvpMap.fromString("root", text);
        assertThat(kvpMap.name()).isEqualTo("root");
        assertKvpMap(kvpMap);
    }

    @ParameterizedTest
    @ValueSource(strings = {"key1@value1|key2@value2", "key1@value1|key2@value2|"})
    public void shouldParseValidStringWithNonDefaultSeparators(String text) {
        KvpSelectorsSuppliers.KvpMap kvpMap =
                KvpSelectorsSuppliers.KvpMap.fromString("root", text, Split.on('|'), Split.on('@'));
        assertThat(kvpMap.name()).isEqualTo("root");
        assertKvpMap(kvpMap);
    }

    private void assertKvpMap(KvpSelectorsSuppliers.KvpMap kvpMap) {
        assertThat(kvpMap.size()).isEqualTo(0);
        assertThat(kvpMap.isScalar()).isFalse();
        assertThat(kvpMap.isArray()).isFalse();
        assertThat(kvpMap.isNull()).isFalse();
        assertThat(kvpMap.has("key1")).isTrue();

        KvpNode key1Value = kvpMap.getProperty("nodeKey1", "key1");
        assertThat(key1Value.name()).isEqualTo("nodeKey1");
        assertThat(key1Value.size()).isEqualTo(0);
        assertThat(key1Value.isScalar()).isTrue();
        assertThat(key1Value.isArray()).isFalse();
        assertThat(key1Value.isNull()).isFalse();
        assertThat(key1Value.has("anyProp")).isFalse();
        assertThat(key1Value.getProperty("anyNode", "anyProp")).isNull();
        assertThat(key1Value.getIndexed("node3", 3, "nodeKey1")).isNull();
        assertThat(key1Value.text()).isEqualTo("value1");

        assertThat(kvpMap.has("key2")).isTrue();

        KvpNode key2Value = kvpMap.getProperty("nodeKey2", "key2");
        assertThat(key2Value.name()).isEqualTo("nodeKey2");
        assertThat(key2Value.size()).isEqualTo(0);
        assertThat(key2Value.isScalar()).isTrue();
        assertThat(key2Value.isArray()).isFalse();
        assertThat(key2Value.isNull()).isFalse();
        assertThat(key2Value.has("anyProp")).isFalse();
        assertThat(key2Value.getProperty("anyNode", "anyProp")).isNull();
        assertThat(key2Value.getIndexed("node3", 3, "nodeKey2")).isNull();
        assertThat(key2Value.text()).isEqualTo("value2");

        assertThat(kvpMap.has("key3")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"key1", "key1;"})
    public void shouldParseKeysOnly(String text) {
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString("root", text);
        assertThat(csvMap.size()).isEqualTo(0);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();

        KvpNode key1 = csvMap.getProperty("nodeKey1", "key1");
        assertThat(key1.name()).isEqualTo("nodeKy1");
        assertThat(key1.size()).isEqualTo(0);
        assertThat(key1.isScalar()).isTrue();
        assertThat(key1.isArray()).isFalse();
        assertThat(key1.isNull()).isFalse();
        assertThat(key1.text()).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   ", "\t", "\n"})
    public void shouldHandleEmptyOrNullString(String text) {
        KvpSelectorsSuppliers.KvpMap csvMap = KvpSelectorsSuppliers.KvpMap.fromString("root", text);
        assertThat(csvMap.size()).isEqualTo(0);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isTrue();
        assertThat(csvMap.text()).isEmpty();
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '$',
            textBlock =
                    """
                        INPUT                   $ EXPECTED                    $ PAIRS_SEP $ KEY_VAL_SEP
                        key1                    $ {key1=}                     $ ;         $ =
                        key1;                   $ {key1=}                     $ ;         $ =
                        key1;key2               $ {key1=, key2=}              $ ;         $ =
                        key1=                   $ {key1=}                     $ ;         $ =
                        key1=value1             $ {key1=value1}               $ ;         $ =
                        key1=value1;            $ {key1=value1}               $ ;         $ =
                        key1=value1;key2=value2 $ {key1=value1, key2=value2}  $ ;         $ =
                        key1=value1;key2        $ {key1=value1, key2=}        $ ;         $ =
                        key1=value1;key2;       $ {key1=value1, key2=}        $ ;         $ =
                        key1=value1;key2=       $ {key1=value1, key2=}        $ ;         $ =
                        key1=value1;=           $ {key1=value1}               $ ;         $ =
                        key1@value1-key2@value2 $ {key1=value1, key2=value2}  $ -         $ @
                        key1@value1|key2@value2 $ {key1=value1, key2=value2}  $ |         $ @
                        """)
    public void shouldReturnAsText(String input, String expected, char pairsSep, char keyValSep) {
        KvpSelectorsSuppliers.KvpMap csvMap =
                KvpSelectorsSuppliers.KvpMap.fromString(
                        "root", input, Split.on(pairsSep), Split.on(keyValSep));
        assertThat(csvMap.size()).isEqualTo(0);
        assertThat(csvMap.isScalar()).isFalse();
        assertThat(csvMap.isArray()).isFalse();
        assertThat(csvMap.isNull()).isFalse();
        assertThat(csvMap.text()).isEqualTo(expected);
    }
}
