
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

package com.lightstreamer.kafka.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.test_utils.ConsumerRecords.record;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class MetaSelectorTest {

    static MetaSelector metaSelector(String expression) {
        return new MetaSelectorSupplier().newSelector("field_name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        EXPRESSION,     VALUE
                        TOPIC,          record-topic
                        PARTITION,      150
                        TIMESTAMP,      -1
                        OFFSET,         120
                    """)
    public void shouldExtractAttribute(String expression, String expectedValue) {
        MetaSelector selector = metaSelector(expression);
        Value value = selector.extract(record("record-key", "record-value"));
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{argumentsWithNames}]")
    @EmptySource
    @NullSource
    @ValueSource(strings = {"NOT-EXISTING-ATTRIBUTE", "PARTITION."})
    public void shouldNotCreate(String expression) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> metaSelector(expression));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Expected the root token [TIMESTAMP|PARTITION|TOPIC|OFFSET] while"
                                + " evaluating [field_name]");
    }
}
