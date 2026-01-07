
/*
 * Copyright (C) 2025 Lightstreamer Srl
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.HEADERS;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.OFFSET;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.PARTITION;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.TIMESTAMP;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.TOPIC;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.from;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Set;

public class ConstantTest {

    @Test
    public void shouldCreateFromValidConstantNames() {
        assertThat(from("KEY")).isEqualTo(KEY);
        assertThat(from("VALUE")).isEqualTo(VALUE);
        assertThat(from("TIMESTAMP")).isEqualTo(TIMESTAMP);
        assertThat(from("PARTITION")).isEqualTo(PARTITION);
        assertThat(from("OFFSET")).isEqualTo(OFFSET);
        assertThat(from("TOPIC")).isEqualTo(TOPIC);
        assertThat(from("HEADERS")).isEqualTo(HEADERS);
    }

    @Test
    public void shouldNotCreateFromInvalidConstant() {
        assertThat(from("INVALID")).isNull();
    }

    @Test
    public void shouldReturnAllConstants() {
        Set<Constant> allConstants = Constant.all();
        Constant[] allValues = Constant.values();
        assertThat(allValues.length).isEqualTo(allConstants.size());
        for (Constant constant : allValues) {
            assertThat(allConstants.contains(constant));
            assertThat(constant.isWildCardExpression()).isFalse();
        }
    }

    @Test
    public void shouldReturnItself() {
        for (Constant constant : Constant.values()) {
            assertThat(constant).isSameInstanceAs(constant.constant());
        }
    }

    @Test
    public void shouldTokenBeEqualToConstantName() {
        for (Constant constant : Constant.values()) {
            String[] tokens = constant.tokens();
            assertThat(tokens.length).isEqualTo(1);
            assertThat(constant.toString()).isEqualTo(tokens[0]);
        }
    }

    @Test
    public void shouldExpressionBeEqualToConstantName() {
        for (Constant constant : Constant.values()) {
            assertThat(constant.toString()).isEqualTo(constant.expression());
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"OFFSET", "PARTITION", "TIMESTAMP", "TOPIC"})
    public void shouldNotAllowIndex(Constant constant) {
        assertThat(constant.allowIndex()).isFalse();
    }

    @Test
    public void shouldNotCreateFromNonIndexableConstants() {
        // By default constants don't allow indexing, so these should return null
        assertThat(from("OFFSET[2]")).isNull();
        assertThat(from("PARTITION[3]")).isNull();
        assertThat(from("TIMESTAMP[4]")).isNull();
        assertThat(from("TOPIC[5]")).isNull();
    }

    @Test
    public void shouldNotCreateFromInvalidIndexedConstants() {
        // Invalid indexed constants should return null
        assertThat(from("INVALID[0]")).isNull();
        assertThat(from("A['index']")).isNull();
    }

    @ParameterizedTest
    @EnumSource(names = {"KEY", "VALUE", "HEADERS"})
    public void shouldAllowIndex(Constant constant) {
        assertThat(constant.allowIndex()).isTrue();
    }

    @Test
    public void shouldCreateFromIndexableConstants() {
        // HEADERS allows indexing, so these should return the constant
        assertThat(from("HEADERS[0]")).isEqualTo(HEADERS);
        assertThat(from("HEADERS[1]")).isEqualTo(HEADERS);
        assertThat(from("HEADERS[a]")).isEqualTo(HEADERS);

        // Even with different indices, it should still return the same constant
        assertThat(from("HEADERS[1][200]")).isEqualTo(HEADERS);

        assertThat(from("KEY[0]")).isEqualTo(KEY);
        assertThat(from("KEY[1][100]")).isEqualTo(KEY);
        assertThat(from("VALUE[1]")).isEqualTo(VALUE);
        assertThat(from("VALUE[1]['attrib']")).isEqualTo(VALUE);
        assertThat(from("VALUE['name']--aaa]")).isEqualTo(VALUE);
    }

    @Test
    public void testValuesStr() {
        String expected =
                Arrays.stream(Constant.values())
                        .map(Enum::toString)
                        .reduce((a, b) -> a + "|" + b)
                        .orElse("");
        assertThat(expected).isEqualTo(Constant.VALUES_STR);
    }
}
