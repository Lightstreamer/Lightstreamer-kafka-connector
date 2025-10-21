
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class DataTest {

    @Test
    public void shouldCreateFromNameAndValue() {
        Data data = Data.from("name", "value");
        assertThat(data.text()).isEqualTo("value");
        assertThat(data.name()).isEqualTo("name");
    }

    static Stream<Arguments> dataArrays() {
        return Stream.of(
                Arguments.of(List.of(), "schema", "schema"),
                Arguments.of(
                        List.of(Data.from("schema", "value1")), "schema", "schema-[schema=value1]"),
                Arguments.of(
                        List.of(Data.from("name1", "value1")), "schema", "schema-[name1=value1]"),
                Arguments.of(
                        List.of(Data.from("name1", "value1"), Data.from("name2", "value2")),
                        "schema",
                        "schema-[name1=value1,name2=value2]"),
                Arguments.of(
                        List.of(
                                Data.from("name1", "value1"),
                                Data.from("name2", "value2"),
                                Data.from("name3", "value3")),
                        "schema",
                        "schema-[name1=value1,name2=value2,name3=value3]"));
    }

    @ParameterizedTest
    @MethodSource("dataArrays")
    public void shouldBuildItemName(List<Data> dataList, String schema, String expected) {
        String result = Data.buildItemName(dataList.toArray(new Data[0]), schema);
        assertThat(result).isEqualTo(expected);
    }
}
