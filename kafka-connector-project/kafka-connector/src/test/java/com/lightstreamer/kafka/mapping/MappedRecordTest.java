
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

package com.lightstreamer.kafka.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.mapping.selectors.Value;
import com.lightstreamer.kafka.mapping.selectors.ValuesContainer;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class MappedRecordTest {

    ValuesExtractor<String, String> extractor1;
    ValuesExtractor<String, String> extractor2;
    Set<ValuesContainer> valuesContainers;

    @BeforeEach
    void creaValuesContainer() {
        extractor1 =
                ValuesExtractor.<String, String>builder()
                        .withSuppliers(TestSelectorSuppliers.string())
                        .withSchemaName("schema1")
                        .withExpressions(Map.of("partition", "PARTITION", "topic", "TOPIC"))
                        .build();

        ValuesContainer container1 =
                ValuesContainer.of(
                        extractor1,
                        Set.of(
                                Value.of("partition", "partitionValue"),
                                Value.of("topic", "topicValue")));

        extractor2 =
                ValuesExtractor.<String, String>builder()
                        .withSchemaName("schema1")
                        .withSuppliers(TestSelectorSuppliers.string())
                        .withExpressions(Map.of("partition2", "PARTITION", "topic2", "TOPIC"))
                        .build();

        ValuesContainer container2 =
                ValuesContainer.of(
                        extractor2,
                        Set.of(
                                Value.of("partition2", "partitionValue2"),
                                Value.of("topic2", "topicValue2")));
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
        Set<ValuesContainer> vc =
                Set.of(ValuesContainer.of(extractor1, Set.of(Value.of("partition", null))));
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
        Set<ValuesContainer> vc =
                Set.of(ValuesContainer.of(extractor1, Set.of(Value.of("partition", null))));
        DefaultMappedRecord mp = new DefaultMappedRecord("topic", vc);
        assertThat(mp.mappedValuesSize()).isEqualTo(1);
    }
}