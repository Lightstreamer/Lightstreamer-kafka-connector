
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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toMap;

import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.utils.Split;
import com.lightstreamer.kafka.common.utils.Split.Pair;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.net.InetSocketAddress;
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

    public static final String LIGHTREAMER_PROXY_ADAPTER_ADDRESS =
            "lightstreamer.server.proxy_adapter.address";
    public static final String LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS =
            "lightstreamer.server.proxy_adapter.socket.connection.setup.timeout.ms";
    public static final String ITEM_TEMPLATES = "item.templates";
    public static final String TOPIC_MAPPINGS = "topic.mappings";
    public static final String FIELD_MAPPINGS = "field.mappings";
    public static final String RECORD_EXTRACTION_ERROR_STRATEGY =
            "record.extraction.error.strategy";

    public static ConfigDef makeConfig() {
        return new ConfigDef()
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTREAMER_PROXY_ADAPTER_ADDRESS)
                                .type(Type.INT)
                                .defaultValue(ConfigDef.NO_DEFAULT_VALUE)
                                .validator(new ProxyAdapterAddressValidator())
                                .importance(Importance.HIGH)
                                .documentation("The Lightstreamer server's proxy adapter address")
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS)
                                .type(Type.INT)
                                .defaultValue(5)
                                .importance(Importance.LOW)
                                .documentation("")
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(ITEM_TEMPLATES)
                                .type(Type.STRING)
                                .defaultValue(null)
                                .validator(new ItemTemplateValidator())
                                .importance(Importance.MEDIUM)
                                .documentation("Item template expressions")
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(TOPIC_MAPPINGS)
                                .type(Type.LIST)
                                .defaultValue(ConfigDef.NO_DEFAULT_VALUE)
                                .validator(new ListValidator())
                                .importance(Importance.HIGH)
                                .documentation("")
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(FIELD_MAPPINGS)
                                .type(Type.LIST)
                                .defaultValue(ConfigDef.NO_DEFAULT_VALUE)
                                .validator(new FieldMappingsValidator())
                                .importance(Importance.HIGH)
                                .documentation("Name of the Lightsteramer fields to be mapped")
                                .build())
                .define(
                        new ConfigKeyBuilder()
                                .name(RECORD_EXTRACTION_ERROR_STRATEGY)
                                .type(Type.STRING)
                                .defaultValue(
                                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE.toString())
                                .validator(RecordErrorHandlingStrategies.VALIDATOR)
                                .recommender(RecordErrorHandlingStrategies.RECOMMENDER)
                                .importance(Importance.LOW)
                                .documentation(
                                        "The error handling strategy to be used if an error occurs while extracting data from incoming deserialized records")
                                .build());
    }

    public LightstreamerConnectorConfig(Map<?, ?> originals) {
        super(makeConfig(), originals);
    }

    public InetSocketAddress getProxyAdapterAddress() {
        return Split.pair(getString(LIGHTREAMER_PROXY_ADAPTER_ADDRESS))
                .map(p -> new InetSocketAddress(p.key(), Integer.valueOf(p.value())))
                .orElseThrow(() -> new RuntimeException());
    }

    public int getSetupConnectionTimeoutMs() {
        return getInt(LIGHTSTREAMER_PROXY_ADAPTER_CONNECTION_SETUP_TIMEOUT_MS);
    }

    public List<TopicMappingConfig> getTopicMappings() {
        return TopicMappingConfig.from(
                getList(TOPIC_MAPPINGS).stream()
                        .flatMap(t -> Split.pair(t).stream())
                        .collect(groupingBy(Pair::key, mapping(Pair::value, joining(",")))));
    }

    public Map<String, String> getFieldMappings() {
        return getList(FIELD_MAPPINGS).stream()
                .flatMap(t -> Split.pair(t).stream())
                .collect(toMap(Pair::key, Pair::value));
    }

    public ItemTemplateConfigs getItemTemplates() {
        return ItemTemplateConfigs.from(
                Split.bySemicolon(getString(ITEM_TEMPLATES)).stream()
                        .flatMap(s -> Split.pair(s).stream())
                        .collect(toMap(Pair::key, Pair::value)));
    }

    public RecordErrorHandlingStrategy getErrRecordErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.valueOf(getString(RECORD_EXTRACTION_ERROR_STRATEGY));
    }
}
