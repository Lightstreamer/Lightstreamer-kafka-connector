
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Set;

public class ConstantTest {

    @Test
    public void shouldCreateFromValidConstantNames() {
        assertThat(Constant.from("KEY")).isEqualTo(Constant.KEY);
        assertThat(Constant.from("VALUE")).isEqualTo(Constant.VALUE);
        assertThat(Constant.from("TIMESTAMP")).isEqualTo(Constant.TIMESTAMP);
        assertThat(Constant.from("PARTITION")).isEqualTo(Constant.PARTITION);
        assertThat(Constant.from("OFFSET")).isEqualTo(Constant.OFFSET);
        assertThat(Constant.from("TOPIC")).isEqualTo(Constant.TOPIC);
        assertThat(Constant.from("HEADERS")).isEqualTo(Constant.HEADERS);
    }

    @Test
    public void shouldNotCreateFromInvalidConstant() {
        assertThat(Constant.from("INVALID")).isNull();
    }

    @Test
    public void shouldReturnAllConstants() {
        Set<Constant> allConstants = Constant.all();
        Constant[] allValues = Constant.values();
        assertThat(allValues.length).isEqualTo(allConstants.size());
        for (Constant constant : allValues) {
            assertThat(allConstants.contains(constant));
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
    @EnumSource(names = {"KEY", "VALUE", "OFFSET", "PARTITION", "TIMESTAMP", "TOPIC"})
    public void shouldNotAllowIndex(Constant constant) {
        assertFalse(constant.allowIndex());
    }

    @Test
    public void shouldNotCreateFromNonIndexableConstants() {
        // By default constants don't allow indexing, so these should return null
        assertThat(Constant.from("KEY[0]")).isNull();
        assertThat(Constant.from("VALUE[1]")).isNull();
        assertThat(null == Constant.from("OFFSET[2]")).isTrue();
        assertThat(Constant.from("PARTITION[3]")).isNull();
        assertThat(Constant.from("TIMESTAMP[4]")).isNull();
        assertThat(Constant.from("TOPIC[5]")).isNull();
    }

    @Test
    public void shouldNotCreateFromInvalidIndexedConstants() {
        // Invalid indexed constants should return null
        assertThat(Constant.from("INVALID[0]")).isNull();
        assertThat(Constant.from("A['index']")).isNull();
    }

    @ParameterizedTest
    @EnumSource(names = {"HEADERS"})
    public void shouldAllowIndex(Constant constant) {
        assertThat(constant.allowIndex()).isTrue();
    }

    @Test
    public void shouldCreateFromIndexableConstants() {
        // HEADERS allows indexing, so these should return the constant
        assertThat(Constant.from("HEADERS[0]")).isEqualTo(Constant.HEADERS);
        assertThat(Constant.from("HEADERS[1]")).isEqualTo(Constant.HEADERS);
        assertThat(Constant.from("HEADERS[a]")).isEqualTo(Constant.HEADERS);

        // Even with different indices, it should still return the same constant
        assertThat(Constant.from("HEADERS[1][200]")).isEqualTo(Constant.HEADERS);
    }

    @Test
    public void testValuesStr() {
        String expected =
                Arrays.stream(Constant.values())
                        .map(Enum::toString)
                        .reduce((a, b) -> a + "|" + b)
                        .orElse("");
        assertEquals(expected, Constant.VALUES_STR);
    }
}
