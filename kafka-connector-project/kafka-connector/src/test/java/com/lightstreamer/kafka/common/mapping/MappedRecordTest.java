
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.DataContainer;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class MappedRecordTest {

    DataExtractor<String, String> extractor1;
    DataExtractor<String, String> extractor2;
    Set<DataContainer> valuesContainers;

    @BeforeEach
    void creaValuesContainer() throws ExtractionException {
        extractor1 =
                DataExtractor.<String, String>builder()
                        .withSuppliers(TestSelectorSuppliers.string())
                        .withSchemaName("schema1")
                        .withExpressions(
                                Map.of(
                                        "partition",
                                        Expressions.expression("PARTITION"),
                                        "topic",
                                        Expressions.expression("TOPIC")))
                        .build();

        DataContainer container1 =
                DataContainer.of(
                        extractor1,
                        Set.of(
                                Data.of("partition", "partitionValue"),
                                Data.of("topic", "topicValue")));

        extractor2 =
                DataExtractor.<String, String>builder()
                        .withSchemaName("schema1")
                        .withSuppliers(TestSelectorSuppliers.string())
                        .withExpressions(
                                Map.of(
                                        "partition2",
                                        Expressions.expression("PARTITION"),
                                        "topic2",
                                        Expressions.expression("TOPIC")))
                        .build();

        DataContainer container2 =
                DataContainer.of(
                        extractor2,
                        Set.of(
                                Data.of("partition2", "partitionValue2"),
                                Data.of("topic2", "topicValue2")));
        valuesContainers = Set.of(container1, container2);
    }

    @Test
    void shouldHaveExpectedTopic() {
        DefaultMappedRecord mp = new DefaultMappedRecord("topic", Collections.emptySet());
        assertThat(mp.topic()).isEqualTo("topic");
    }

    @Test
    void shouldFilter() {
        DefaultMappedRecord mp = new DefaultMappedRecord("topic", valuesContainers);
        Map<String, String> map1 = mp.filter(extractor1);
        assertThat(map1).containsExactly("partition", "partitionValue", "topic", "topicValue");

        Map<String, String> map2 = mp.filter(extractor2);
        assertThat(map2).containsExactly("partition2", "partitionValue2", "topic2", "topicValue2");
    }

    @Test
    void shouldFilterWithNullValue() {
        Set<DataContainer> vc =
                Set.of(DataContainer.of(extractor1, Set.of(Data.of("partition", null))));
        DefaultMappedRecord mp = new DefaultMappedRecord("topic", vc);
        Map<String, String> map1 = mp.filter(extractor1);
        assertThat(map1).containsExactly("partition", null);
    }

    @Test
    void shouldHaveExpectedMappedValueSize() {
        DefaultMappedRecord mp = new DefaultMappedRecord("topic", valuesContainers);
        assertThat(mp.mappedValuesSize()).isEqualTo(4);
    }

    @Test
    void shouldHaveExpectedMappedValueSizeWithNullValue() {
        Set<DataContainer> vc =
                Set.of(DataContainer.of(extractor1, Set.of(Data.of("partition", null))));
        DefaultMappedRecord mp = new DefaultMappedRecord("topic", vc);
        assertThat(mp.mappedValuesSize()).isEqualTo(1);
    }
}
