
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

import com.google.common.io.Files;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClientOptions;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LightstreamerConnectorConfigTest {

    static Map<String, String> basicConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "host:6661");
        config.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "item1");
        config.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, "field1:#{VALUE}");
        return config;
    }

    @Test
    void shouldConfigWithRequiredParameters() {
        // No lightstreamer.server.proxy.adapter.address specified
        Map<String, String> props = new HashMap<>();
        ConfigException ce =
                assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required configuration \"lightstreamer.server.proxy_adapter.address\" which has no default value.");

        // Put valid address and go on checking
        props.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS,
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

        // Invalid field mappings
        props.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, "field1:value1");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Invalid value for configuration \"field.mappings\": Field expression must be expressed in the form #{...}");

        // Put valid field mappings and go on checking
        props.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, "field1:#{VALUE}");
        assertDoesNotThrow(() -> new LightstreamerConnectorConfig(props));
    }

    @Test
    void shouldGetProxyAdapterClientOptions() {
        Map<String, String> props = basicConfig();
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        ProxyAdapterClientOptions proxyAdapterClientOptions = config.getProxyAdapterClientOptions();

        assertThat(proxyAdapterClientOptions.hostname).isEqualTo("host");
        assertThat(proxyAdapterClientOptions.port).isEqualTo(6661);
        assertThat(proxyAdapterClientOptions.timeout).isEqualTo(5000);

        Map<String, String> updateConfigs = new HashMap<>(basicConfig());
        updateConfigs.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "hostname:6662");
        updateConfigs.put(
                LightstreamerConnectorConfig
                        .LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS,
                "10000");
        config = new LightstreamerConnectorConfig(updateConfigs);

        proxyAdapterClientOptions = config.getProxyAdapterClientOptions();

        assertThat(proxyAdapterClientOptions.hostname).isEqualTo("hostname");
        assertThat(proxyAdapterClientOptions.port).isEqualTo(6662);
        assertThat(proxyAdapterClientOptions.timeout).isEqualTo(10000);
    }

    @Test
    void shouldGetMoreItemTemplates() {
        Map<String, String> props = basicConfig();
        props.put(
                LightstreamerConnectorConfig.ITEM_TEMPLATES,
                "stock-template:stock-#{index=KEY};product-template:product-#{id=KEY,price=VALUE.price}");

        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        ItemTemplateConfigs itemTemplate = config.getItemTemplates();

        Map<String, TemplateExpression> expressions = itemTemplate.expressions();
        assertThat(expressions).hasSize(2);

        TemplateExpression stockExpression = itemTemplate.getExpression("stock-template");
        assertThat(stockExpression.prefix()).isEqualTo("stock");
        assertThat(stockExpression.params())
                .containsExactly("index", Expressions.expression("KEY"));

        TemplateExpression productExpression = itemTemplate.getExpression("product-template");
        assertThat(productExpression.prefix()).isEqualTo("product");
        assertThat(productExpression.params())
                .containsExactly(
                        "id",
                        Expressions.expression("KEY"),
                        "price",
                        Expressions.expression("VALUE.price"));
    }

    @Test
    void shouldGetTopicMappings() {
        // stocks -> [item-template.stock-template, item-template-stock-template-2]
        // orders -> [item-template.order-template, oreder-item]
        Map<String, String> props = basicConfig();
        props.put(
                LightstreamerConnectorConfig.TOPIC_MAPPINGS,
                "stocks:item-template.stock-template,stocks:item-template.stock-template-2,orders:item-template.order-template,orders:order-item");
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        List<TopicMappingConfig> topicMappings = config.getTopicMappings();
        assertThat(topicMappings).hasSize(2);

        TopicMappingConfig orderTopicMapping = topicMappings.get(0);
        assertThat(orderTopicMapping.topic()).isEqualTo("orders");
        assertThat(orderTopicMapping.mappings())
                .containsExactly("item-template.order-template", "order-item");

        TopicMappingConfig stockTopicMappingConfig = topicMappings.get(1);
        assertThat(stockTopicMappingConfig.topic()).isEqualTo("stocks");
        assertThat(stockTopicMappingConfig.mappings())
                .containsExactly("item-template.stock-template", "item-template.stock-template-2");
    }

    @Test
    void shouldGetFieldMappings() {
        String fieldMappingConfig =
                """
                    timestamp:#{VALUE.timestamp},\
                    time:#{VALUE.time},\
                    stock_name: #{VALUE.name}   ,\
                    last_price:#{VALUE.last_price},\
                    ask:#{VALUE.ask},\
                    ask_quantity:#{VALUE.ask_quantity},\
                    bid:#{VALUE.bid},\
                    bid_quantity:   #{VALUE.bid_quantity}  ,\
                    pct_change:#{VALUE.pct_change},\
                    min:#{VALUE.min},\
                    max:#{VALUE.max},\
                    ref_price:#{VALUE.ref_price},\
                    open_price:#{VALUE.open_price},\
                    item_status:#{VALUE.item_status},\
                    ts:#{TIMESTAMP},\
                    topic:#{TOPIC},\
                    offset:#{OFFSET},\
                    partition:#{PARTITION}
                """;
        Map<String, String> props = basicConfig();
        props.put(LightstreamerConnectorConfig.FIELD_MAPPINGS, fieldMappingConfig);
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);

        FieldConfigs fieldMappings = config.getFieldConfigs();

        assertThat(fieldMappings.expressions())
                .containsExactly(
                        "timestamp",
                        Expressions.expression("VALUE.timestamp"),
                        "time",
                        Expressions.expression("VALUE.time"),
                        "stock_name",
                        Expressions.expression("VALUE.name"),
                        "last_price",
                        Expressions.expression("VALUE.last_price"),
                        "ask",
                        Expressions.expression("VALUE.ask"),
                        "ask_quantity",
                        Expressions.expression("VALUE.ask_quantity"),
                        "bid",
                        Expressions.expression("VALUE.bid"),
                        "bid_quantity",
                        Expressions.expression("VALUE.bid_quantity"),
                        "pct_change",
                        Expressions.expression("VALUE.pct_change"),
                        "min",
                        Expressions.expression("VALUE.min"),
                        "max",
                        Expressions.expression("VALUE.max"),
                        "ref_price",
                        Expressions.expression("VALUE.ref_price"),
                        "open_price",
                        Expressions.expression("VALUE.open_price"),
                        "item_status",
                        Expressions.expression("VALUE.item_status"),
                        "ts",
                        Expressions.expression("TIMESTAMP"),
                        "topic",
                        Expressions.expression("TOPIC"),
                        "offset",
                        Expressions.expression("OFFSET"),
                        "partition",
                        Expressions.expression("PARTITION"));
    }

    @ParameterizedTest
    @EnumSource
    void shouldGetRecordErrorHandlingStrategyFromConfig(RecordErrorHandlingStrategy strategy) {
        Map<String, String> props = basicConfig();
        props.put(
                LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY, strategy.toString());
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        assertThat(config.getErrRecordErrorHandlingStrategy()).isEqualTo(strategy);
    }

    @ParameterizedTest
    @ValueSource(strings = {"IGNORE_AND_CONTINUE", "FORWARD_TO_DLQ", "TERMINATE_TASK"})
    public void shouldRetrieveRecordErrorHandlingStrategy(String strategy) {
        RecordErrorHandlingStrategy from = RecordErrorHandlingStrategy.from(strategy);
        assertThat(from.toString()).isEqualTo(strategy);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"NO_VALID_STRATEGY"})
    public void shouldNotRetrieveRecordErrorHandlingStrategy(String noValidStrategy) {
        RecordErrorHandlingStrategy from = RecordErrorHandlingStrategy.from(noValidStrategy);
        assertThat(from).isNull();
    }

    @Test
    public void shouldToHtmml() throws IOException {
        ConfigDef config = LightstreamerConnectorConfig.makeConfig();
        Files.write(config.toHtml().getBytes(), new File("config.html"));
        Files.write(config.toEnrichedRst().getBytes(), new File("config.rst"));
    }
}
