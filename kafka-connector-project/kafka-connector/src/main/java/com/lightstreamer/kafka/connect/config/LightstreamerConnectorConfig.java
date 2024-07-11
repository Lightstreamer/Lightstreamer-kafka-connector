
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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public class LightstreamerConnectorConfig extends AbstractConfig {

    public enum RecordErrorHandlingStrategy {
        IGNORE_AND_CONTINUE,
        FORWARD_TO_DLQ,
        TERMINATE_TASK;

        private static final Map<String, RecordErrorHandlingStrategy> NAME_CACHE;

        static {
            NAME_CACHE =
                    Stream.of(values())
                            .collect(toMap(RecordErrorHandlingStrategy::toString, identity()));
        }

        static RecordErrorHandlingStrategy from(String name) {
            return NAME_CACHE.get(name);
        }
    }

    public static final String LIGHTREAMER_PROXY_ADAPTER_ADDRESS =
            "lightstreamer.server.proxy_adapter_address";
    public static final String ITEM_TEMPLATES = "item.templates";
    public static final String TOPIC_MAPPINGS = "topic.mappings";
    public static final String FIELD_MAPPINGS = "field.mappings";
    public static final String RECORD_EXTRACTION_ERROR_STRATEGY =
            "record.extraction.error.strategy";

    public static ConfigDef makeConfig() {
        return new ConfigDef()
                .define(
                        LIGHTREAMER_PROXY_ADAPTER_ADDRESS,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new ProxyAdapterAddressValidator(),
                        Importance.HIGH,
                        "The Lightstreamer server's proxy adapter address")
                .define(
                        ITEM_TEMPLATES,
                        ConfigDef.Type.STRING,
                        null,
                        new ItemTemplateValidator(),
                        ConfigDef.Importance.MEDIUM,
                        "Item template expressions")
                .define(
                        TOPIC_MAPPINGS,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new TopicMappingsValidator(),
                        ConfigDef.Importance.HIGH,
                        "")
                .define(
                        FIELD_MAPPINGS,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new FieldMappingsValidator(),
                        ConfigDef.Importance.HIGH,
                        "Name of the Lightsteramer fields to be mapped")
                .define(
                        RECORD_EXTRACTION_ERROR_STRATEGY,
                        ConfigDef.Type.STRING,
                        RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE.toString(),
                        new RecordErrorHandlingStrategyValidator(),
                        ConfigDef.Importance.MEDIUM,
                        "The error handling strategy to be used if an error occurs while extracting data from incoming deserialized records");
    }

    private Map<String, String> topicMappings;

    public LightstreamerConnectorConfig(Map<?, ?> originals) {
        super(makeConfig(), originals);
    }

    public Map<String, String> getTopicMappings() {
        return getList(TOPIC_MAPPINGS).stream()
                .collect(
                        groupingBy(
                                s -> s.split(":")[0], mapping(s -> s.split(":")[1], joining(","))));
    }

    public Map<String, String> getItemTemplates() {
        String it = getString(ITEM_TEMPLATES);
        if (it != null) {
            return Arrays.stream(it.split(";"))
                    .collect(toMap(s -> s.split(":")[0], s -> s.split(":")[1]));
        }
        return Collections.emptyMap();
    }

    public RecordErrorHandlingStrategy getErrRecordErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.valueOf(getString(RECORD_EXTRACTION_ERROR_STRATEGY));
    }
}
