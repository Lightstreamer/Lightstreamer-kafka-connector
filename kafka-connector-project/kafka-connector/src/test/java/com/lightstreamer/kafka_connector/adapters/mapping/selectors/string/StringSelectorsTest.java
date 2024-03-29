
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

package com.lightstreamer.kafka_connector.adapters.mapping.selectors.string;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords.fromKey;
import static com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords.fromValue;

import static org.junit.Assert.assertThrows;

import com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class StringSelectorsTest {

    static ValueSelector<String> valueSelector(String expression) {
        return StringSelectorSuppliers.valueSelectorSupplier().newSelector("name", expression);
    }

    static KeySelector<String> keySelector(String expression) {
        return StringSelectorSuppliers.keySelectorSupplier().newSelector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION, EXPECTED
                        VALUE,      joe
                        VALUE,      alex
                        """)
    public void shouldExtractValue(String expression, String expected) {
        ValueSelector<String> selector = valueSelector(expression);
        assertThat(selector.name()).isEqualTo("name");
        assertThat(selector.extract(fromValue(expected)).text()).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION, EXPECTED
                        KEY,        joe
                        KEY,        alex
                        """)
    public void shouldExtractKey(String expression, String expected) {
        KeySelector<String> selector = keySelector(expression);
        assertThat(selector.name()).isEqualTo("name");
        assertThat(selector.extract(fromKey(expected)).text()).isEqualTo(expected);
    }

    @Test
    public void shouldNotCreate() {
        ExpressionException e1 =
                assertThrows(ExpressionException.class, () -> keySelector("invalidKey"));
        assertThat(e1.getMessage())
                .isEqualTo("Expected the root token [KEY] while evaluating [name]");

        ExpressionException e2 = assertThrows(ExpressionException.class, () -> keySelector(""));
        assertThat(e2.getMessage())
                .isEqualTo("Expected the root token [KEY] while evaluating [name]");

        ExpressionException e3 =
                assertThrows(ExpressionException.class, () -> valueSelector("invalidValue"));
        assertThat(e3.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [name]");

        ExpressionException e4 = assertThrows(ExpressionException.class, () -> valueSelector(""));
        assertThat(e4.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [name]");
    }
}
