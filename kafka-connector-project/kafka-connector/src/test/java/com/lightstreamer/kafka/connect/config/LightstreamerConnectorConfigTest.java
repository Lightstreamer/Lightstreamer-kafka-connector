
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

package com.lightstreamer.kafka.connect.config;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

public class LightstreamerConnectorConfigTest {

    static Map<String, String> basicConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(LightstreamerConnectorConfig.LIGHTREAMER_PROXY_ADAPTER_ADDRESS, "host:6661");
        config.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "item1");
        config.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, "field1:#{VALUE}");
        return config;
    }

    @Test
    void shouldConfig() {
        // No lightstreamer.server.proxy.adapter.address specified
        Map<String, String> props = new HashMap<>();
        ConfigException ce =
                assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required configuration \"lightstreamer.server.proxy_adapter_address\" which has no default value.");

        // Put valid address and go on checking
        props.put(
                LightstreamerConnectorConfig.LIGHTREAMER_PROXY_ADAPTER_ADDRESS,
                "lighstreamer_host:6661");

        // No topic.mappings
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required configuration \"topic.mappings\" which has no default value.");

        // Empty topic.mappings
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Invalid value for configuration \"topic.mappings\": Must be a non-empty list");

        // List of empty topic.mappings
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, ",");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Invalid value for configuration \"topic.mappings\": Must be a list of non-empty strings");

        // List of mixed non-empty/empty-strings
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "item1,");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Invalid value for configuration \"topic.mappings\": Must be a list of non-empty strings");

        // Put valid topic mappings and go on checking
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "item1,item-template.template1");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required configuration \"field.mappings\" which has no default value.");

        // Empty field.mappings
        props.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, "");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Invalid value for configuration \"field.mappings\": Must be a non-empty list");

        // Put valid field mappings and go on checking
        props.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, "field1:value1");
        assertDoesNotThrow(() -> new LightstreamerConnectorConfig(props));
    }

    @Test
    void shouldGetMoreItemTemplates() {
        Map<String, String> props = basicConfig();
        props.put(
                LightstreamerConnectorConfig.ITEM_TEMPLATES,
                "stock-template:stock-#{index=KEY};product-template:product-#{id=KEY,price=VALUE.price}");

        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        Map<String, String> itemTemplate = config.getItemTemplates();
        assertThat(itemTemplate)
                .containsExactly(
                        "stock-template",
                        "stock-#{index=KEY}",
                        "product-template",
                        "product-#{id=KEY,price=VALUE.price}");
    }

    @Test
    void shouldGetTopicMappings() {
        Map<String, String> props = basicConfig();

        props.put(
                LightstreamerConnectorConfig.TOPIC_MAPPINGS,
                "stocks:item-template.stock-template,stocks:item-template.stock-template-2,orders:item-template.order-template");
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        Map<String, String> topicMappings = config.getTopicMappings();

        assertThat(topicMappings).containsKey("stocks");
        assertThat(topicMappings.get("stocks"))
                .isEqualTo("item-template.stock-template,item-template.stock-template-2");

        assertThat(topicMappings).containsKey("orders");
        assertThat(topicMappings.get("orders")).isEqualTo("item-template.order-template");
    }

    @ParameterizedTest
    @EnumSource
    void shouldGetErrorStrategy(RecordErrorHandlingStrategy strategy) {
        Map<String, String> props = basicConfig();
        props.put(
                LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY, strategy.toString());
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        assertThat(config.getErrRecordErrorHandlingStrategy()).isEqualTo(strategy);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"invalid"})
    void shouldFailDueToInvalidErrorStrategy(String strategy) {
        Map<String, String> props = basicConfig();
        props.put(LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY, strategy);

        ConfigException ce =
                assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Invalid value invalid for configuration record.extraction.error.strategy");
    }
}
