
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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.utils.Split;
import com.lightstreamer.kafka.common.utils.Split.Pair;
import com.lightstreamer.kafka.connect.proxy.ProxyAdapterClientOptions;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class LightstreamerConnectorConfig extends AbstractConfig {

    public enum RecordErrorHandlingStrategy {
        IGNORE_AND_CONTINUE,
        FORWARD_TO_DLQ,
        TERMINATE_TASK;

        private static final Map<String, RecordErrorHandlingStrategy> NAME_CACHE;
        private static final List<Object> RECOMMENDED;

        static {
            NAME_CACHE =
                    Stream.of(values())
                            .collect(toMap(RecordErrorHandlingStrategy::toString, identity()));
            RECOMMENDED = Arrays.asList(NAME_CACHE.keySet().toArray(new Object[0]));
        }

        static RecordErrorHandlingStrategy from(String name) {
            return NAME_CACHE.get(name);
        }

        static List<Object> recommended() {
            return RECOMMENDED;
        }
    }

    public static final String LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS =
            "lightstreamer.server.proxy_adapter.address";
    public static final String LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS_DOC =
            "The Lightstreamer server's Proxy Adapter address to connect to in the format host:port.";

    public static final String LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS =
            "lightstreamer.server.proxy_adapter.socket.connection.setup.timeout.ms";
    public static final String LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS_DOC =
            "The (optional) value in milliseconds for the time to wait while trying to establish a "
                    + "connection to the Lighstreamer server's Proxy Adapter before terminating the task."
                    + "\nSpecify 0 for infinite timeout.";

    public static final String LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES =
            "lightstreamer.server.proxy_adapter.socket.connection.setup.max.retries";
    public static final String LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES_DOC =
            "The (optional) max number of retries to establish a connection the Lighstreamer server's Proxy Adapter.";

    public static final String LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS =
            "lightstreamer.server.proxy_adapter.socket.connection.setup.retry.delay.ms";
    public static final String LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS_DOC =
            "The (optional) amount of time in milliseconds to wait before retrying to establish a new connection in case of failure."
                    + "\nOnly applicable if 'lightstreamer.server.proxy_adapter.socket.connection.setup.max.retries' > 0.";

    public static final String LIGHTSTREAMER_PROXY_ADAPTER_USERNAME =
            "lightstreamer.server.proxy_adapter.username";
    public static final String LIGHTSTREAMER_PROXY_ADAPTER_USERNAME_DOC =
            "The username to use for authenticating to the Lightstreamer server's Proxy Adapter. "
                    + "This setting requires authentication to be enabled in the configuration of the Proxy Adapter.";

    public static final String LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD =
            "lightstreamer.server.proxy_adapter.password";
    public static final String LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD_DOC =
            "The password to use for authenticating to the Lightstreamer server's Proxy Adapter. "
                    + "This setting requires authentication to be enabled in the configuration of the Proxy Adapter.";

    public static final String ITEM_TEMPLATES = "item.templates";
    public static final String ITEM_TEMPLATES_DOC =
            """
             Semicolon-separated list of _item templates_, which specify the rules to enable the _filtering routing_. The list should describe a set of templates in the following form:

             [templateName1]:[template1];[templateName2]:[template2];...;[templateNameN]:[templateN]

             where the [templateX] configures the item template [templaeName] defining the general format of the items the Lightstremer clients must subscribe to to receive udpdates.

             A template is specified in the form:

             item-prefix-#{paramName1=extractionExpression1,paramName2=extractionExpression2,...}

             To map a topic to an item template, reference it using the item-template prefix in the topic.mappings configuration:

             topic.mappings=some-topic:item-template.templateName1,item-template.templateName2,...
             """;

    public static final String TOPIC_MAPPINGS = "topic.mappings";
    public static final String TOPIC_MAPPINGS_DOC =
            "Semicolon-separated list of mappings between source topics and Lightstreamer items. The list should describe a set of "
                    + "mappings in the form:"
                    + "\n\n"
                    + "[topicName1]:[mappingList1];[topicName2]:[mappingList2];...[topicNameN]:[mappingListN]"
                    + "\n\n"
                    + "where every specified topic ([topicNameX]) is mapped to the item names or item templates specified as "
                    + " comma-separated list ([mappingListX]).";

    public static final String RECORD_MAPPING = "record.mapping";
    public static final String RECORD_MAPPINGS_DOC =
            "The list of mapping between Kafa records and Ligtstreamer fields. The list should describe a set of "
                    + "subscribable fields in the following form:"
                    + "\n\n"
                    + "[fieldName1]:[extractionExpression1],[fieldName2]:[extractionExpressionN],...,[fieldNameN]:[extractionExpressionN]"
                    + "\n\n"
                    + "where the Lightstreamer field [fieldNameX] whill hold the data extracted from a deserialized Kafka record using the "
                    + "Data Extraction Language [extractionExpressionX].";

    public static final String RECORD_EXTRACTION_ERROR_STRATEGY =
            "record.extraction.error.strategy";
    public static final String RECORD_EXTRACTION_ERROR_STRATEGY_DOC =
            """
            The (optional) error handling strategy to be used if an error occurs while extracting data from incoming deserialized records. Can be one of the following:

            - TERMINATE_TASK: terminate the task immediately
            - IGNORE_AND_CONTINUE: ignore the error and continue to process the next record
            - FORWARD_TO_DLQ: forward the record to the dead letter queue

            In particular, the FORWARD_TO_DLQ value requires a dead letter queue to be configured; otherwise it will fallback to TERMINATE_TASK.
            """;

    public static ConfigDef makeConfig() {
        return new ConfigDef()
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS)
                                .type(Type.STRING)
                                .defaultValue(ConfigDef.NO_DEFAULT_VALUE)
                                .validator(new ProxyAdapterAddressValidator())
                                .importance(Importance.HIGH)
                                .documentation(LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS)
                                .type(Type.INT)
                                .defaultValue(5000)
                                .validator(Range.atLeast(0))
                                .importance(Importance.LOW)
                                .documentation(
                                        LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES)
                                .type(Type.INT)
                                .defaultValue(1)
                                .validator(Range.atLeast(0))
                                .importance(Importance.MEDIUM)
                                .documentation(
                                        LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS)
                                .type(Type.LONG)
                                .defaultValue(5000)
                                .validator(Range.atLeast(0))
                                .importance(Importance.LOW)
                                .documentation(
                                        LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTSTREAMER_PROXY_ADAPTER_USERNAME)
                                .type(Type.STRING)
                                .defaultValue(null)
                                .importance(Importance.MEDIUM)
                                .documentation(LIGHTSTREAMER_PROXY_ADAPTER_USERNAME_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD)
                                .type(Type.PASSWORD)
                                .defaultValue(null)
                                .importance(Importance.MEDIUM)
                                .documentation(LIGHTSTREAMER_PROXY_ADAPTER_PASSWORD_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(ITEM_TEMPLATES)
                                .type(Type.STRING)
                                .defaultValue(null)
                                .validator(new ItemTemplateValidator())
                                .importance(Importance.MEDIUM)
                                .documentation(ITEM_TEMPLATES_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(TOPIC_MAPPINGS)
                                .type(Type.STRING)
                                .defaultValue(ConfigDef.NO_DEFAULT_VALUE)
                                .validator(new TopicMappingsValidator())
                                .importance(Importance.HIGH)
                                .documentation(TOPIC_MAPPINGS_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(RECORD_MAPPING)
                                .type(Type.LIST)
                                .defaultValue(ConfigDef.NO_DEFAULT_VALUE)
                                .validator(new RecordMappingValidator())
                                .importance(Importance.HIGH)
                                .documentation(RECORD_MAPPINGS_DOC)
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(RECORD_EXTRACTION_ERROR_STRATEGY)
                                .type(Type.STRING)
                                .defaultValue(
                                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE.toString())
                                .validator(RecordErrorHandlingStrategies.VALIDATOR)
                                .recommender(RecordErrorHandlingStrategies.RECOMMENDER)
                                .importance(Importance.MEDIUM)
                                .documentation(RECORD_EXTRACTION_ERROR_STRATEGY_DOC)
                                .build());
    }

    private final ItemTemplateConfigs itemTemplateConfigs;
    private final List<TopicMappingConfig> topicMppingCofigs;
    private final FieldConfigs fieldConfigs;
    private final ProxyAdapterClientOptions proxyAdapterClientOptions;

    public LightstreamerConnectorConfig(Map<?, ?> originals) {
        super(makeConfig(), originals);

        itemTemplateConfigs = initItemTemplateConfigs();
        topicMppingCofigs = initTopicMappingConfigs();
        fieldConfigs = initFieldConfigs();

        Pair address = getProxyAdapterAddress();
        proxyAdapterClientOptions =
                new ProxyAdapterClientOptions.Builder()
                        .hostname(address.key())
                        .port(Integer.valueOf(address.value()))
                        .connectionTimeout(getSetupConnectionTimeoutMs())
                        .connectionMaxRetries(getSetupConnectionMaxRetries())
                        .connectionRetryDelayMs(getSetupConnectionRetryDelayMs())
                        .build();
    }

    public ProxyAdapterClientOptions getProxyAdapterClientOptions() {
        return proxyAdapterClientOptions;
    }

    public int getSetupConnectionTimeoutMs() {
        return getInt(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS);
    }

    public int getSetupConnectionMaxRetries() {
        return getInt(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_MAX_RETRIES);
    }

    public long getSetupConnectionRetryDelayMs() {
        return getLong(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_RETRY_DELAY_MS);
    }

    public List<TopicMappingConfig> getTopicMappings() {
        return topicMppingCofigs;
    }

    public FieldConfigs getFieldConfigs() {
        return fieldConfigs;
    }

    public ItemTemplateConfigs getItemTemplateConfigs() {
        return itemTemplateConfigs;
    }

    public RecordErrorHandlingStrategy getErrRecordErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.valueOf(getString(RECORD_EXTRACTION_ERROR_STRATEGY));
    }

    private Pair getProxyAdapterAddress() {
        return Split.asPair(getString(LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS))
                .orElseThrow(() -> new RuntimeException());
    }

    private FieldConfigs initFieldConfigs() {
        return FieldConfigs.from(
                getList(RECORD_MAPPING).stream()
                        .flatMap(t -> Split.asPair(t).stream())
                        .collect(toMap(Pair::key, Pair::value)));
    }

    private List<TopicMappingConfig> initTopicMappingConfigs() {
        return TopicMappingConfig.from(
                Split.bySemicolon(getString(TOPIC_MAPPINGS)).stream()
                        .flatMap(t -> Split.asPair(t).stream())
                        .collect(toMap(Pair::key, Pair::value)));
    }

    private ItemTemplateConfigs initItemTemplateConfigs() {
        try {
            return ItemTemplateConfigs.from(
                    Split.bySemicolon(getString(ITEM_TEMPLATES)).stream()
                            .flatMap(s -> Split.asPair(s).stream())
                            .collect(toMap(Pair::key, Pair::value)));
        } catch (ConfigException ce) {
            throw new org.apache.kafka.common.config.ConfigException("");
        }
    }
}
