
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

package com.lightstreamer.kafka_connector.adapters.config;

import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.BOOL;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.ERROR_STRATEGY;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.INT;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.URL;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.DefaultHolder.defaultValue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;

import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;

import java.io.File;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public final class ConnectorConfig extends AbstractConfig {

    public static final String ENABLED = "enabled";

    public static final String DATA_ADAPTER_NAME = "data_provider.name";

    public static final String ITEM_TEMPLATE = "item-template";

    public static final String TOPIC_MAPPING = "map";
    private static final String MAP_SUFFIX = "to";

    public static final String FIELD_MAPPING = "field";

    public static final String KEY_EVALUATOR_TYPE = "key.evaluator.type";

    public static final String KEY_SCHEMA_FILE = "key.evaluator.schema.file";

    public static final String VALUE_EVALUATOR_TYPE = "value.evaluator.type";

    public static final String VALUE_SCHEMA_FILE = "value.evaluator.schema.file";

    public static final String GROUP_ID = "group.id";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String KEY_EVALUATOR_SCHEMA_REGISTRY_URL =
            "key.evaluator.schema.registry.url";

    public static final String VALUE_EVALUATOR_SCHEMA_REGISTRY_URL =
            "value.evaluator.schema.registry.url";

    public static final String ITEM_INFO_NAME = "info.item";

    public static final String ITEM_INFO_FIELD = "info.field";

    public static final String RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY =
            "record.extraction.error.strategy";

    // Kafka consumer specific settings
    private static final String CONNECTOR_PREFIX = "consumer.";
    public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG =
            CONNECTOR_PREFIX + AUTO_OFFSET_RESET_CONFIG;
    public static final String CONSUMER_ENABLE_AUTO_COMMIT_CONFIG =
            CONNECTOR_PREFIX + ENABLE_AUTO_COMMIT_CONFIG;
    public static final String CONSUMER_FETCH_MIN_BYTES_CONFIG =
            CONNECTOR_PREFIX + FETCH_MIN_BYTES_CONFIG;
    public static final String CONSUMER_FETCH_MAX_BYTES_CONFIG =
            CONNECTOR_PREFIX + FETCH_MAX_BYTES_CONFIG;
    public static final String CONSUMER_FETCH_MAX_WAIT_MS_CONFIG =
            CONNECTOR_PREFIX + FETCH_MAX_WAIT_MS_CONFIG;
    public static final String CONSUMER_MAX_POLL_RECORDS =
            CONNECTOR_PREFIX + MAX_POLL_RECORDS_CONFIG;
    public static final String CONSUMER_RECONNECT_BACKOFF_MS_CONFIG =
            CONNECTOR_PREFIX + RECONNECT_BACKOFF_MS_CONFIG;
    public static final String CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG =
            CONNECTOR_PREFIX + RECONNECT_BACKOFF_MAX_MS_CONFIG;
    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS =
            CONNECTOR_PREFIX + HEARTBEAT_INTERVAL_MS_CONFIG;
    public static final String CONSUMER_SESSION_TIMEOUT_MS =
            CONNECTOR_PREFIX + SESSION_TIMEOUT_MS_CONFIG;
    public static final String CONSUMER_MAX_POLL_INTERVAL_MS =
            CONNECTOR_PREFIX + MAX_POLL_INTERVAL_MS_CONFIG;
    public static final String CONSUMER_METADATA_MAX_AGE_CONFIGS =
            CONNECTOR_PREFIX + METADATA_MAX_AGE_CONFIG;

    private static final ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigSpec()
                        .add(ADAPTERS_CONF_ID, true, false, TEXT)
                        .add(ENABLED, false, false, BOOL, defaultValue("true"))
                        .add(ADAPTER_DIR, true, false, ConfType.DIRECTORY)
                        .add(BOOTSTRAP_SERVERS, true, false, ConfType.HOST_LIST)
                        .add(
                                GROUP_ID,
                                false,
                                false,
                                TEXT,
                                defaultValue(
                                        params -> {
                                            String suffix =
                                                    new SecureRandom()
                                                            .ints(20, 48, 122)
                                                            .mapToObj(Character::toString)
                                                            .collect(Collectors.joining());
                                            return "%s-%s-%s"
                                                    .formatted(
                                                            params.get(ADAPTERS_CONF_ID),
                                                            params.get(DATA_ADAPTER_NAME),
                                                            suffix);
                                        }))
                        .add(DATA_ADAPTER_NAME, true, false, TEXT)
                        .add(ITEM_TEMPLATE, true, true, TEXT)
                        .add(TOPIC_MAPPING, true, true, MAP_SUFFIX, TEXT)
                        .add(FIELD_MAPPING, true, true, TEXT)
                        .add(KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, URL)
                        .add(VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, URL)
                        .add(
                                KEY_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(EvaluatorType.STRING.toString()))
                        .add(KEY_SCHEMA_FILE, false, false, FILE)
                        .add(
                                VALUE_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(EvaluatorType.STRING.toString()))
                        .add(VALUE_SCHEMA_FILE, false, false, FILE)
                        .add(ITEM_INFO_NAME, false, false, TEXT, defaultValue("INFO"))
                        .add(ITEM_INFO_FIELD, false, false, TEXT, defaultValue("MSG"))
                        .add(
                                RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY,
                                false,
                                false,
                                ERROR_STRATEGY,
                                defaultValue("IGNORE_AND_CONTINUE"))
                        .add(
                                CONSUMER_AUTO_OFFSET_RESET_CONFIG,
                                false,
                                false,
                                TEXT,
                                defaultValue("latest"))
                        .add(
                                CONSUMER_ENABLE_AUTO_COMMIT_CONFIG,
                                false,
                                false,
                                BOOL,
                                false,
                                defaultValue("false"))
                        .add(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG, false, false, INT)
                        .add(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MIN_BYTES_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MIN_BYTES_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MAX_BYTES_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG, false, false, INT)
                        .add(CONSUMER_MAX_POLL_RECORDS, false, false, INT)
                        .add(CONSUMER_HEARTBEAT_INTERVAL_MS, false, false, INT)
                        .add(CONSUMER_SESSION_TIMEOUT_MS, false, false, INT)
                        .add(
                                CONSUMER_MAX_POLL_INTERVAL_MS,
                                false,
                                false,
                                INT,
                                false,
                                defaultValue("5000"))
                        .add(
                                CONSUMER_METADATA_MAX_AGE_CONFIGS,
                                false,
                                false,
                                INT,
                                false,
                                defaultValue("250"));
    }

    private ConnectorConfig(ConfigSpec spec, Map<String, String> configs) {
        super(spec, configs);
    }

    public ConnectorConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);
    }

    static ConfigSpec configSpec() {
        return CONFIG_SPEC;
    }

    public static ConnectorConfig newConfig(File adapterDir, Map<String, String> params) {
        return new ConnectorConfig(
                AbstractConfig.appendAdapterDir(CONFIG_SPEC, params, adapterDir));
    }

    public Properties baseConsumerProps() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, getHostsList(BOOTSTRAP_SERVERS));
        properties.setProperty(GROUP_ID_CONFIG, getText(GROUP_ID));
        copySetting(properties, METADATA_MAX_AGE_CONFIG, getInt(CONSUMER_METADATA_MAX_AGE_CONFIGS));
        copySetting(
                properties, AUTO_OFFSET_RESET_CONFIG, getText(CONSUMER_AUTO_OFFSET_RESET_CONFIG));
        copySetting(
                properties,
                ENABLE_AUTO_COMMIT_CONFIG,
                getBoolean(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG));
        copySetting(properties, FETCH_MIN_BYTES_CONFIG, getInt(CONSUMER_FETCH_MIN_BYTES_CONFIG));
        copySetting(properties, FETCH_MAX_BYTES_CONFIG, getInt(CONSUMER_FETCH_MAX_BYTES_CONFIG));
        copySetting(
                properties, FETCH_MAX_WAIT_MS_CONFIG, getInt(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG));
        copySetting(
                properties, HEARTBEAT_INTERVAL_MS_CONFIG, getInt(CONSUMER_HEARTBEAT_INTERVAL_MS));
        copySetting(properties, MAX_POLL_RECORDS_CONFIG, getInt(CONSUMER_MAX_POLL_RECORDS));
        copySetting(
                properties,
                RECONNECT_BACKOFF_MAX_MS_CONFIG,
                getInt(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG));
        copySetting(
                properties,
                RECONNECT_BACKOFF_MS_CONFIG,
                getInt(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG));
        copySetting(properties, SESSION_TIMEOUT_MS_CONFIG, getInt(CONSUMER_SESSION_TIMEOUT_MS));
        copySetting(properties, MAX_POLL_INTERVAL_MS_CONFIG, getInt(CONSUMER_MAX_POLL_INTERVAL_MS));

        return properties;
    }

    private void copySetting(Properties properties, String toKey, String value) {
        if (value != null) {
            properties.setProperty(toKey, value);
        }
    }

    public Map<String, ?> extendsConsumerProps(Map<String, String> props) {
        Map<String, String> extendedProps =
                new HashMap<>(
                        baseConsumerProps().entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                e -> e.getKey().toString(),
                                                e -> e.getValue().toString())));
        extendedProps.putAll(props);
        return extendedProps;
    }

    public final EvaluatorType getEvaluator(String configKey) {
        return EvaluatorType.valueOf(get(configKey, EVALUATOR, false));
    }

    public final RecordErrorHandlingStrategy getRecordExtractionErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.valueOf(
                get(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, ERROR_STRATEGY, false));
    }

    public boolean hasKeySchemaFile() {
        return getFile(KEY_SCHEMA_FILE) != null;
    }

    public boolean hasValueSchemaFile() {
        return getFile(VALUE_SCHEMA_FILE) != null;
    }

    public String getMetadataAdapterName() {
        return getText(ADAPTERS_CONF_ID);
    }

    public String getAdapterName() {
        return getText(DATA_ADAPTER_NAME);
    }

    public boolean isEnabled() {
        return getBoolean(ENABLED).equals("true");
    }

    public String getItemInfoName() {
        return getText(ITEM_INFO_NAME);
    }

    public String getItemInfoField() {
        return getText(ITEM_INFO_FIELD);
    }
}