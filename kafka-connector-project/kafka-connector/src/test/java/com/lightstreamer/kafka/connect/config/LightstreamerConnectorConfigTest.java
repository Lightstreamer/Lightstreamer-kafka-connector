
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.io.Files;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.connect.client.ProxyAdapterClientOptions;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.connect.server.ProviderServerOptions;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Disabled;
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
        config.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "topic:item1");
        config.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE}");
        return config;
    }

    @Test
    void shouldConfigWithRequiredParameters() {
        // No lightstreamer.server.proxy.adapter.address specified
        Map<String, String> props = new HashMap<>();
        ConfigException ce =
                assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required configuration \"lightstreamer.server.proxy_adapter.address\" which has no default value.");

        // Put valid address and go on checking
        props.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS,
                "lightstreamer_host:6661");

        // No topic.mappings
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required configuration \"topic.mappings\" which has no default value.");

        // Empty topic.mappings
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value for configuration \"topic.mappings\": Must be a non-empty semicolon-separated list");

        // List of empty topic.mappings
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, ",");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value for configuration \"topic.mappings\": Each entry must be in the form [topicName]:[mappingList]");

        // List of mixed non-empty/empty-strings
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "item1,");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value for configuration \"topic.mappings\": Each entry must be in the form [topicName]:[mappingList]");

        // Put valid topic mappings and go on checking
        props.put(
                LightstreamerConnectorConfig.TOPIC_MAPPINGS, "topic:item1,item-template.template1");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required configuration \"record.mappings\" which has no default value.");

        // Empty record.mapping
        props.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value for configuration \"record.mappings\": Must be a non-empty list");

        // Invalid field mappings
        props.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:value1");
        ce = assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value for configuration \"record.mappings\": Extraction expression must be in the form #{...}");

        // Put valid field mappings and go on checking
        props.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE}");
        assertDoesNotThrow(() -> new LightstreamerConnectorConfig(props));
    }

    @Test
    void shouldNotValidateNegativeConnectionSetupTimeout() {
        Map<String, String> updateConfigs = new HashMap<>(basicConfig());
        updateConfigs.put(
                LightstreamerConnectorConfig
                        .LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS,
                "-1");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> new LightstreamerConnectorConfig(updateConfigs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value -1 for configuration lightstreamer.server.proxy_adapter.socket.connection.setup.timeout.ms: Value must be at least 0");
    }

    @Test
    void shouldNotValidateNegativeConnectionSetupMaxRetries() {
        Map<String, String> updateConfigs = new HashMap<>(basicConfig());
        updateConfigs.put(
                LightstreamerConnectorConfig
                        .LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES,
                "-1");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> new LightstreamerConnectorConfig(updateConfigs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value -1 for configuration lightstreamer.server.proxy_adapter.socket.connection.setup.max.retries: Value must be at least 0");
    }

    @Test
    void shouldNotValidateNegativeConnectionSetupRetryDelay() {
        Map<String, String> updateConfigs = new HashMap<>(basicConfig());
        updateConfigs.put(
                LightstreamerConnectorConfig
                        .LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS,
                "-1");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> new LightstreamerConnectorConfig(updateConfigs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value -1 for configuration lightstreamer.server.proxy_adapter.socket.connection.setup.retry.delay.ms: Value must be at least 0");
    }

    @Test
    void shouldGetProxyAdapterClientOptions() {
        Map<String, String> props = basicConfig();
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        assertThat(config.isConnectionInversionEnabled()).isFalse();
        assertThat(config.getProviderServerOptions()).isNull();

        ProxyAdapterClientOptions options = config.getProxyAdapterClientOptions();
        assertThat(options.hostname).isEqualTo("host");
        assertThat(options.port).isEqualTo(6661);
        assertThat(options.connectionTimeout).isEqualTo(5000);
        assertThat(options.connectionMaxRetries).isEqualTo(1);
        assertThat(options.connectionRetryDelayMs).isEqualTo(5000);
        assertThat(options.username).isNull();
        assertThat(options.password).isNull();

        Map<String, String> updateConfigs = new HashMap<>(basicConfig());
        updateConfigs.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "hostname:6662");
        updateConfigs.put(
                LightstreamerConnectorConfig
                        .LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS,
                "10000");
        updateConfigs.put(
                LightstreamerConnectorConfig
                        .LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES,
                "5");
        updateConfigs.put(
                LightstreamerConnectorConfig
                        .LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS,
                "15000");
        updateConfigs.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_USERNAME, "user");
        updateConfigs.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD, "password");

        config = new LightstreamerConnectorConfig(updateConfigs);

        options = config.getProxyAdapterClientOptions();

        assertThat(options.hostname).isEqualTo("hostname");
        assertThat(options.port).isEqualTo(6662);
        assertThat(options.connectionTimeout).isEqualTo(10000);
        assertThat(options.connectionMaxRetries).isEqualTo(5);
        assertThat(options.connectionRetryDelayMs).isEqualTo(15000);
        assertThat(options.username).isEqualTo("user");
        assertThat(options.password).isEqualTo("password");
    }

    @Test
    void shouldGetProviderServerOptions() {
        Map<String, String> props = basicConfig();
        props.put(LightstreamerConnectorConfig.CONNECTION_INVERSION_ENABLE, "true");
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        assertThat(config.isConnectionInversionEnabled()).isTrue();
        assertThat(config.getProxyAdapterClientOptions()).isNull();

        ProviderServerOptions options = config.getProviderServerOptions();
        assertThat(options.port).isEqualTo(6661);
        assertThat(options.maxProxyAdapterConnections).isEqualTo(1);
        assertThat(options.username).isNull();
        assertThat(options.password).isNull();

        Map<String, String> updateConfigs = new HashMap<>(basicConfig());
        updateConfigs.put(LightstreamerConnectorConfig.CONNECTION_INVERSION_ENABLE, "true");
        updateConfigs.put(LightstreamerConnectorConfig.REQUEST_REPLY_PORT, "6662");
        updateConfigs.put(LightstreamerConnectorConfig.MAX_PROXY_ADAPTER_CONNECTIONS, "10");
        updateConfigs.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_USERNAME, "user");
        updateConfigs.put(
                LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD, "password");

        config = new LightstreamerConnectorConfig(updateConfigs);
        options = config.getProviderServerOptions();

        assertThat(options.port).isEqualTo(6662);
        assertThat(options.maxProxyAdapterConnections).isEqualTo(10);
        assertThat(options.username).isEqualTo("user");
        assertThat(options.password).isEqualTo("password");
    }

    @Test
    void shouldNotValidateNonPositiveMaxProxyAdapterConnections() {
        Map<String, String> updateConfigs = new HashMap<>(basicConfig());
        updateConfigs.put(LightstreamerConnectorConfig.CONNECTION_INVERSION_ENABLE, "true");
        updateConfigs.put(LightstreamerConnectorConfig.MAX_PROXY_ADAPTER_CONNECTIONS, "0");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> new LightstreamerConnectorConfig(updateConfigs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value 0 for configuration max.proxy.adapter.connections: Value must be at least 1");
    }

    @Test
    void shouldGetMoreItemTemplates() {
        Map<String, String> props = basicConfig();
        props.put(
                LightstreamerConnectorConfig.ITEM_TEMPLATES,
                "stock-template:stock-#{index=KEY};product-template:product-#{id=KEY,price=VALUE.price}");

        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        ItemTemplateConfigs itemTemplate = config.getItemTemplateConfigs();

        Map<String, TemplateExpression> expressions = itemTemplate.templates();
        assertThat(expressions).hasSize(2);

        TemplateExpression stockExpression = itemTemplate.getTemplateExpression("stock-template");
        assertThat(stockExpression.prefix()).isEqualTo("stock");
        assertThat(stockExpression.params()).containsExactly("index", Expression("KEY"));

        TemplateExpression productExpression =
                itemTemplate.getTemplateExpression("product-template");
        assertThat(productExpression.prefix()).isEqualTo("product");
        assertThat(productExpression.params())
                .containsExactly("id", Expression("KEY"), "price", Expression("VALUE.price"));
    }

    @Test
    void shouldGetTopicMappings() {
        // stocks -> [item-template.stock-template, stocks-item, item-template.stock-template-2]
        // orders -> [item-template.order-template, order-item]
        Map<String, String> props = basicConfig();
        props.put(
                LightstreamerConnectorConfig.TOPIC_MAPPINGS,
                "stocks:stocks-item,item-template.stock-template,item-template.stock-template-2;orders:item-template.order-template, order-item");
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
                .containsExactly(
                        "item-template.stock-template",
                        "stocks-item",
                        "item-template.stock-template-2");
    }

    @Test
    public void shouldGetTopicMappingRegex() {
        Map<String, String> props = basicConfig();
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        assertThat(config.isRegexEnabled()).isFalse();

        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS_REGEX_ENABLE, "true");
        config = new LightstreamerConnectorConfig(props);
        assertThat(config.isRegexEnabled()).isTrue();
    }

    @Test
    public void shouldNotValidateInvalidTopicMappingRegex() {
        Map<String, String> props = basicConfig();
        props.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS_REGEX_ENABLE, "INVALID");
        ConfigException ce =
                assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value INVALID for configuration topic.mappings.regex.enable: Expected value to be either true or false");
    }

    @Test
    public void shouldGetRecordMappingSkipFailed() {
        Map<String, String> props = basicConfig();
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        assertThat(config.isRecordMappingSkipFailedEnabled()).isFalse();

        props.put(LightstreamerConnectorConfig.RECORD_MAPPINGS_SKIP_FAILED_ENABLE, "true");
        config = new LightstreamerConnectorConfig(props);
        assertThat(config.isRecordMappingSkipFailedEnabled()).isTrue();
    }

    @Test
    public void shouldGetRecordMappingMapNonScalarValues() {
        Map<String, String> props = basicConfig();
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        assertThat(config.isRecordMappingMapNonScalarValuesEnabled()).isFalse();

        props.put(
                LightstreamerConnectorConfig.RECORD_MAPPINGS_MAP_NON_SCALAR_VALUES_ENABLE, "true");
        config = new LightstreamerConnectorConfig(props);
        assertThat(config.isRecordMappingMapNonScalarValuesEnabled()).isTrue();
    }

    @Test
    public void shouldNotValidateInvalidRecordMappingSkipFailed() {
        Map<String, String> props = basicConfig();
        props.put(LightstreamerConnectorConfig.RECORD_MAPPINGS_SKIP_FAILED_ENABLE, "INVALID");
        ConfigException ce =
                assertThrows(ConfigException.class, () -> new LightstreamerConnectorConfig(props));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value INVALID for configuration record.mappings.skip.failed.enable: Expected value to be either true or false");
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
                    bid_quantity:   #{VALUE.bid_quantity},\
                    pct_change:#{VALUE.pct_change},\
                    min:#{VALUE.min},\
                    max:#{VALUE.max},\
                    ref_price:#{VALUE.ref_price},\
                    open_price:#{VALUE.open_price},\
                    item_status:#{VALUE.item_status},\
                    ts:#{TIMESTAMP},\
                    topic:#{TOPIC},\
                    offset:#{OFFSET},\
                    partition:#{PARTITION},\
                    *:#{VALUE}
                """;
        Map<String, String> props = basicConfig();
        props.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, fieldMappingConfig);
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);

        FieldConfigs fieldMappings = config.getFieldConfigs();

        assertThat(fieldMappings.boundExpressions())
                .containsExactly(
                        "timestamp",
                        Expression("VALUE.timestamp"),
                        "time",
                        Expression("VALUE.time"),
                        "stock_name",
                        Expression("VALUE.name"),
                        "last_price",
                        Expression("VALUE.last_price"),
                        "ask",
                        Expression("VALUE.ask"),
                        "ask_quantity",
                        Expression("VALUE.ask_quantity"),
                        "bid",
                        Expression("VALUE.bid"),
                        "bid_quantity",
                        Expression("VALUE.bid_quantity"),
                        "pct_change",
                        Expression("VALUE.pct_change"),
                        "min",
                        Expression("VALUE.min"),
                        "max",
                        Expression("VALUE.max"),
                        "ref_price",
                        Expression("VALUE.ref_price"),
                        "open_price",
                        Expression("VALUE.open_price"),
                        "item_status",
                        Expression("VALUE.item_status"),
                        "ts",
                        Expression("TIMESTAMP"),
                        "topic",
                        Expression("TOPIC"),
                        "offset",
                        Expression("OFFSET"),
                        "partition",
                        Expression("PARTITION"));
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

    @Disabled
    @Test
    public void shouldProduceFormattedConfiguration() throws IOException {
        ConfigDef config = LightstreamerConnectorConfig.makeConfig();
        Files.write(config.toHtml().getBytes(), new File("config.html"));
        Files.write(config.toEnrichedRst().getBytes(), new File("config_enriched.rst"));
        Files.write(config.toRst().getBytes(), new File("config.rst"));
    }
}
