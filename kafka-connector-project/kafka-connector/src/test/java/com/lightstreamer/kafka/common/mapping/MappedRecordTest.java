
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

import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class MappedRecordTest {

    DataExtractor<String, String> extractor1;
    DataExtractor<String, String> extractor2;
    Set<SchemaAndValues> dataContainers;

    @BeforeEach
    void creaValuesContainer() throws ExtractionException {
        // extractor1 =
        //         DataExtractor.<String, String>builder()
        //                 .withSuppliers(TestSelectorSuppliers.string())
        //                 .withSchemaName("schema1")
        //                 .withExpressions(
        //                         Map.of(
        //                                 "partition",
        //                                 Expressions.expression("PARTITION"),
        //                                 "topic",
        //                                 Expressions.expression("TOPIC")))
        //                 .build();

        //                 SchemaAndValues container1 =
        //         DataContainer.from(
        //                 extractor1, Map.of("partition", "partitionValue", "topic",
        // "topicValue"));

        // extractor2 =
        //         DataExtractor.<String, String>builder()
        //                 .withSchemaName("schema1")
        //                 .withSuppliers(TestSelectorSuppliers.string())
        //                 .withExpressions(
        //                         Map.of(
        //                                 "partition2",
        //                                 Expressions.expression("PARTITION"),
        //                                 "topic2",
        //                                 Expressions.expression("TOPIC")))
        //                 .build();

        //                 SchemaAndValues container2 =
        //         SchemaAndValues.from(
        //                 extractor2,
        //                 Map.of(
        //                         "partition2", "partitionValue2",
        //                         "topic2", "topicValue2"));
        // dataContainers = Set.of(container1, container2);
    }

    @Test
    void shouldRoute() {}

    //     @Test
    // FIXME shouldHaveExpectedTopic
    //     void shouldHaveExpectedTopic() {
    //         DefaultMappedRecord mp = new DefaultMappedRecord("topic", Collections.emptySet());
    //         assertThat(mp.topic()).isEqualTo("topic");
    //     }

    // @Test
    //     FIXME shouldFilter
    //         void shouldFilter() {
    //             DefaultMappedRecord mp = new DefaultMappedRecord("topic", dataContainers);
    //             Map<String, String> map1 = mp.filter(extractor1);
    //             assertThat(map1).containsExactly("partition", "partitionValue", "topic",
    //     "topicValue");

    //             Map<String, String> map2 = mp.filter(extractor2);
    //             assertThat(map2).containsExactly("partition2", "partitionValue2", "topic2",
    //     "topicValue2");
    // }

    //     @Test
    // FIXME shouldFilterWithNullValue
    //     void shouldFilterWithNullValue() {
    //         Set<DataContainer> dc =
    //                 Set.of(DataContainer.from(extractor1, Set.of(Data.from("partition", null))));
    //         DefaultMappedRecord mp = new DefaultMappedRecord("topic", dc);
    //         Map<String, String> map1 = mp.filter(extractor1);
    //         assertThat(map1).containsExactly("partition", null);
    //     }

    //     @Test
    // FIXME shouldHaveExpectedMappedValueSize
    //     void shouldHaveExpectedMappedValueSize() {
    //         DefaultMappedRecord mp = new DefaultMappedRecord("topic", dataContainers);
    //         assertThat(mp.mappedValuesSize()).isEqualTo(4);
    //     }

    //     @Test
    // FIXME shouldHaveExpectedMappedValueSizeWithNullValue
    //     void shouldHaveExpectedMappedValueSizeWithNullValue() {
    //         Set<DataContainer> dc =
    //                 Set.of(DataContainer.from(extractor1, Set.of(Data.from("partition", null))));
    //         DefaultMappedRecord mp = new DefaultMappedRecord("topic", dc);
    //         assertThat(mp.mappedValuesSize()).isEqualTo(1);
    //     }
}
