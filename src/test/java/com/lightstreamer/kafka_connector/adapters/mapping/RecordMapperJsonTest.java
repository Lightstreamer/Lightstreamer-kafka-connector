
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

package com.lightstreamer.kafka_connector.adapters.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConsumerRecords;
import com.lightstreamer.kafka_connector.adapters.test_utils.JsonNodeProvider;
import com.lightstreamer.kafka_connector.adapters.test_utils.SelectorsSuppliers;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class RecordMapperJsonTest {

    private static Selectors<String, JsonNode> selectors(
            String schemaName, Map<String, String> entries) {
        return Selectors.from(
                SelectorsSuppliers.jsonValue(ConnectorConfigProvider.minimal()),
                schemaName,
                entries);
    }

    private static Builder<String, JsonNode> builder() {
        return RecordMapper.<String, JsonNode>builder();
    }

    @Tag("unit")
    @Test
    public void shouldBuildEmptyMapper() {
        RecordMapper<String, JsonNode> mapper = builder().build();
        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(0);
    }

    @Tag("unit")
    @Test
    public void shouldBuildMapperWithDuplicateSelectors() {
        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(1);
    }

    @Tag("unit")
    @Test
    public void shouldBuildMapperWithDifferentSelectors() {
        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "PARTITION")))
                        .build();

        assertThat(mapper).isNotNull();
        assertThat(mapper.selectorsSize()).isEqualTo(2);
    }

    @Tag("integration")
    @Test
    public void shoulMapEmpty() {
        RecordMapper<String, JsonNode> mapper = builder().build();

        ConsumerRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        // No expected values because no selectors have been bound to the RecordMapper.
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
    }

    @Tag("integration")
    @Test
    public void shoulMapWithValues() {
        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
                        .withSelectors(selectors("test2", Map.of("aKey", "TOPIC")))
                        .withSelectors(selectors("test3", Map.of("aKey", "TIMESTAMP")))
                        .build();

        ConsumerRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
    }

    @Tag("integration")
    @Test
    public void shoulNotFilterDueToUnboundSelectors() {
        RecordMapper<String, JsonNode> mapper =
                builder().withSelectors(selectors("test", Map.of("name", "PARTITION"))).build();

        ConsumerRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);

        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
        Selectors<String, JsonNode> unbonudSelectors =
                selectors("test", Map.of("name", "VALUE.any"));
        assertThat(mappedRecord.filter(unbonudSelectors)).isEmpty();
    }

    @Tag("integration")
    @Test
    public void shouldFilter() {
        Selectors<String, JsonNode> nameSelectors = selectors("test", Map.of("name", "VALUE.name"));

        Selectors<String, JsonNode> childSelectors1 =
                selectors("test", Map.of("firstChildName", "VALUE.children[0].name"));

        Selectors<String, JsonNode> childSelectors2 =
                selectors(
                        "test",
                        Map.of(
                                "secondChildName",
                                "VALUE.children[1].name",
                                "grandChildName",
                                "VALUE.children[1].children[1].name"));

        RecordMapper<String, JsonNode> mapper =
                builder()
                        .withSelectors(nameSelectors)
                        .withSelectors(childSelectors1)
                        .withSelectors(childSelectors2)
                        .build();

        ConsumerRecord<String, JsonNode> kafkaRecord =
                ConsumerRecords.record("", JsonNodeProvider.RECORD);
        MappedRecord mappedRecord = mapper.map(kafkaRecord);
        assertThat(mappedRecord.mappedValuesSize()).isEqualTo(4);

        Map<String, String> parentName = mappedRecord.filter(nameSelectors);
        assertThat(parentName).containsExactly("name", "joe");

        Map<String, String> firstChildName = mappedRecord.filter(childSelectors1);
        assertThat(firstChildName).containsExactly("firstChildName", "alex");

        Map<String, String> otherPeopleNames = mappedRecord.filter(childSelectors2);
        assertThat(otherPeopleNames)
                .containsExactly("secondChildName", "anna", "grandChildName", "terence");
    }
}
