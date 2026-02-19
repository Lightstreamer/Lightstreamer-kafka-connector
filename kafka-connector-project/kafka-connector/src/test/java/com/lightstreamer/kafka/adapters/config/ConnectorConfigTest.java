
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

package com.lightstreamer.kafka.adapters.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.AbstractConfig.ADAPTERS_CONF_ID;
import static com.lightstreamer.kafka.adapters.config.AbstractConfig.ADAPTER_DIR;
import static com.lightstreamer.kafka.adapters.config.BrokerAuthenticationConfigs.PASSWORD;
import static com.lightstreamer.kafka.adapters.config.BrokerAuthenticationConfigs.USERNAME;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.AUTHENTICATION_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.BOOTSTRAP_SERVERS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_CLIENT_ID;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_FETCH_MAX_BYTES_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_FETCH_MIN_BYTES_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_HEARTBEAT_INTERVAL_MS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_MAX_POLL_INTERVAL_MS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_METADATA_MAX_AGE_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_SESSION_TIMEOUT_MS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.DATA_ADAPTER_NAME;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ENCRYPTION_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.FIELDS_AUTO_COMMAND_MODE_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.FIELDS_EVALUATE_AS_COMMAND_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.GROUP_ID;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ITEM_INFO_FIELD;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ITEM_INFO_NAME;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ITEM_TEMPLATE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.LIGHTSTREAMER_CLIENT_ID;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_AT_CONNECTOR_STARTUP_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_FROM;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_MAX_POLL_RECORDS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_NUM_THREADS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_ORDER_STRATEGY;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.TOPIC_MAPPING;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_NAME;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.ENABLE_BASIC_AUTHENTICATION;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.HOSTNAME_VERIFICATION_ENABLE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.KEYSTORE_ENABLE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.KEYSTORE_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.KEYSTORE_PATH;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.KEYSTORE_TYPE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.KEY_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.SSL_CIPHER_SUITES;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.SSL_ENABLED_PROTOCOLS;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.SSL_PROTOCOL;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.TRUSTSTORE_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.TRUSTSTORE_PATH;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.TRUSTSTORE_TYPE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.URL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.STRING;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom.EARLIEST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol.TLSv12;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol.TLSv13;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedNoWildcardCheck;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class ConnectorConfigTest {

    private Path adapterDir;

    private Path avroKeySchemaFile;
    private Path avroValueSchemaFile;

    private Path protoKeySchemaFile;
    private Path protoValueSchemaFile;

    private Path trustStoreFile;
    private Path keyStoreFile;

    private Path keyTabFile;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        avroKeySchemaFile = Files.createTempFile(adapterDir, "key-schema-", ".avsc");
        avroValueSchemaFile = Files.createTempFile(adapterDir, "value-schema-", ".avsc");
        protoKeySchemaFile = Files.createTempFile(adapterDir, "proto-key-schema-", ".desc");
        protoValueSchemaFile = Files.createTempFile(adapterDir, "proto-value-schema-", ".desc");
        trustStoreFile = Files.createTempFile(adapterDir, "truststore", ".jks");
        keyStoreFile = Files.createTempFile(adapterDir, "keystore", ".jks");
        keyTabFile = Files.createTempFile(adapterDir, "keytabFile", ".keytab");
    }

    @AfterEach
    public void after() throws IOException {
        Files.delete(avroKeySchemaFile);
        Files.delete(avroValueSchemaFile);
        Files.delete(protoKeySchemaFile);
        Files.delete(protoValueSchemaFile);
        Files.delete(trustStoreFile);
        Files.delete(keyStoreFile);
        Files.delete(keyTabFile);
        Files.delete(adapterDir);
    }

    @Test
    public void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = ConnectorConfig.configSpec();

        ConfParameter adapterConfId = configSpec.getParameter(ADAPTERS_CONF_ID);
        assertThat(adapterConfId.name()).isEqualTo(ADAPTERS_CONF_ID);
        assertThat(adapterConfId.required()).isTrue();
        assertThat(adapterConfId.multiple()).isFalse();
        assertThat(adapterConfId.mutable()).isTrue();
        assertThat(adapterConfId.defaultValue()).isNull();
        assertThat(adapterConfId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter adapterPath = configSpec.getParameter(ADAPTER_DIR);
        assertThat(adapterPath.name()).isEqualTo(ADAPTER_DIR);
        assertThat(adapterPath.required()).isTrue();
        assertThat(adapterPath.multiple()).isFalse();
        assertThat(adapterPath.mutable()).isTrue();
        assertThat(adapterPath.defaultValue()).isNull();
        assertThat(adapterPath.type()).isEqualTo(ConfType.DIRECTORY);

        ConfParameter dataAdapterName = configSpec.getParameter(DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.name()).isEqualTo(DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.required()).isTrue();
        assertThat(dataAdapterName.multiple()).isFalse();
        assertThat(dataAdapterName.mutable()).isTrue();
        assertThat(dataAdapterName.defaultValue()).isNull();
        assertThat(dataAdapterName.type()).isEqualTo(ConfType.TEXT);

        ConfParameter enabled = configSpec.getParameter(ENABLE);
        assertThat(enabled.name()).isEqualTo(ENABLE);
        assertThat(enabled.required()).isFalse();
        assertThat(enabled.multiple()).isFalse();
        assertThat(enabled.mutable()).isTrue();
        assertThat(enabled.defaultValue()).isEqualTo("true");
        assertThat(enabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter bootStrapServers = configSpec.getParameter(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.name()).isEqualTo(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.required()).isTrue();
        assertThat(bootStrapServers.multiple()).isFalse();
        assertThat(bootStrapServers.mutable()).isTrue();
        assertThat(bootStrapServers.defaultValue()).isNull();
        assertThat(bootStrapServers.type()).isEqualTo(ConfType.HOST_LIST);

        ConfParameter groupId = configSpec.getParameter(GROUP_ID);
        assertThat(groupId.name()).isEqualTo(GROUP_ID);
        assertThat(groupId.required()).isFalse();
        assertThat(groupId.multiple()).isFalse();
        assertThat(groupId.mutable()).isTrue();
        assertThat(groupId.defaultValue()).isNotNull();
        assertThat(groupId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemTemplate = configSpec.getParameter(ITEM_TEMPLATE);
        assertThat(itemTemplate.name()).isEqualTo(ITEM_TEMPLATE);
        assertThat(itemTemplate.required()).isFalse();
        assertThat(itemTemplate.multiple()).isTrue();
        assertThat(itemTemplate.suffix()).isNull();
        assertThat(itemTemplate.mutable()).isTrue();
        assertThat(itemTemplate.defaultValue()).isNull();
        assertThat(itemTemplate.type()).isEqualTo(ConfType.TEXT);

        ConfParameter topicMapping = configSpec.getParameter(TOPIC_MAPPING);
        assertThat(topicMapping.name()).isEqualTo(TOPIC_MAPPING);
        assertThat(topicMapping.required()).isTrue();
        assertThat(topicMapping.multiple()).isTrue();
        assertThat(topicMapping.suffix()).isEqualTo("to");
        assertThat(topicMapping.mutable()).isTrue();
        assertThat(topicMapping.defaultValue()).isNull();
        assertThat(topicMapping.type()).isEqualTo(ConfType.TEXT_LIST);

        ConfParameter mapRegExEnable = configSpec.getParameter(ConnectorConfig.MAP_REG_EX_ENABLE);
        assertThat(mapRegExEnable.name()).isEqualTo(ConnectorConfig.MAP_REG_EX_ENABLE);
        assertThat(mapRegExEnable.required()).isFalse();
        assertThat(mapRegExEnable.multiple()).isFalse();
        assertThat(mapRegExEnable.suffix()).isNull();
        assertThat(mapRegExEnable.mutable()).isTrue();
        assertThat(mapRegExEnable.defaultValue()).isEqualTo("false");
        assertThat(mapRegExEnable.type()).isEqualTo(ConfType.BOOL);

        ConfParameter fieldMapping = configSpec.getParameter(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.name()).isEqualTo(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.required()).isTrue();
        assertThat(fieldMapping.multiple()).isTrue();
        assertThat(fieldMapping.suffix()).isNull();
        assertThat(fieldMapping.mutable()).isTrue();
        assertThat(fieldMapping.defaultValue()).isNull();
        assertThat(fieldMapping.type()).isEqualTo(ConfType.TEXT);

        ConfParameter fieldsSkipFailedMappingEnable =
                configSpec.getParameter(ConnectorConfig.FIELDS_SKIP_FAILED_MAPPING_ENABLE);
        assertThat(fieldsSkipFailedMappingEnable.name())
                .isEqualTo(ConnectorConfig.FIELDS_SKIP_FAILED_MAPPING_ENABLE);
        assertThat(fieldsSkipFailedMappingEnable.required()).isFalse();
        assertThat(fieldsSkipFailedMappingEnable.multiple()).isFalse();
        assertThat(fieldsSkipFailedMappingEnable.suffix()).isNull();
        assertThat(fieldsSkipFailedMappingEnable.mutable()).isTrue();
        assertThat(fieldsSkipFailedMappingEnable.defaultValue()).isEqualTo("false");
        assertThat(fieldsSkipFailedMappingEnable.type()).isEqualTo(ConfType.BOOL);

        ConfParameter fieldsMapNonScalarValuesEnable =
                configSpec.getParameter(ConnectorConfig.FIELDS_MAP_NON_SCALAR_VALUES_ENABLE);
        assertThat(fieldsMapNonScalarValuesEnable.name())
                .isEqualTo(ConnectorConfig.FIELDS_MAP_NON_SCALAR_VALUES_ENABLE);
        assertThat(fieldsMapNonScalarValuesEnable.required()).isFalse();
        assertThat(fieldsMapNonScalarValuesEnable.multiple()).isFalse();
        assertThat(fieldsMapNonScalarValuesEnable.suffix()).isNull();
        assertThat(fieldsMapNonScalarValuesEnable.mutable()).isTrue();
        assertThat(fieldsMapNonScalarValuesEnable.defaultValue()).isEqualTo("false");
        assertThat(fieldsMapNonScalarValuesEnable.type()).isEqualTo(ConfType.BOOL);

        ConfParameter fieldEvaluateCommandEnabled =
                configSpec.getParameter(FIELDS_EVALUATE_AS_COMMAND_ENABLE);
        assertThat(fieldEvaluateCommandEnabled.name()).isEqualTo(FIELDS_EVALUATE_AS_COMMAND_ENABLE);
        assertThat(fieldEvaluateCommandEnabled.required()).isFalse();
        assertThat(fieldEvaluateCommandEnabled.multiple()).isFalse();
        assertThat(fieldEvaluateCommandEnabled.mutable()).isTrue();
        assertThat(fieldEvaluateCommandEnabled.defaultValue()).isEqualTo("false");
        assertThat(fieldEvaluateCommandEnabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter transformToCommandEnabled =
                configSpec.getParameter(FIELDS_AUTO_COMMAND_MODE_ENABLE);
        assertThat(transformToCommandEnabled.name()).isEqualTo(FIELDS_AUTO_COMMAND_MODE_ENABLE);
        assertThat(transformToCommandEnabled.required()).isFalse();
        assertThat(transformToCommandEnabled.multiple()).isFalse();
        assertThat(transformToCommandEnabled.mutable()).isTrue();
        assertThat(transformToCommandEnabled.defaultValue()).isEqualTo("false");
        assertThat(transformToCommandEnabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter keyEvaluatorType = configSpec.getParameter(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.name()).isEqualTo(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.required()).isFalse();
        assertThat(keyEvaluatorType.multiple()).isFalse();
        assertThat(keyEvaluatorType.mutable()).isTrue();
        assertThat(keyEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(keyEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter keySchemaPath = configSpec.getParameter(RECORD_KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaPath.name()).isEqualTo(RECORD_KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaPath.required()).isFalse();
        assertThat(keySchemaPath.multiple()).isFalse();
        assertThat(keySchemaPath.mutable()).isTrue();
        assertThat(keySchemaPath.defaultValue()).isNull();
        assertThat(keySchemaPath.type()).isEqualTo(ConfType.FILE);

        ConfParameter schemaRegistryEnabledForKey =
                configSpec.getParameter(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForKey.name())
                .isEqualTo(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForKey.required()).isFalse();
        assertThat(schemaRegistryEnabledForKey.multiple()).isFalse();
        assertThat(schemaRegistryEnabledForKey.mutable()).isTrue();
        assertThat(schemaRegistryEnabledForKey.defaultValue()).isEqualTo("false");
        assertThat(schemaRegistryEnabledForKey.type()).isEqualTo(ConfType.BOOL);

        ConfParameter valueEvaluatorType = configSpec.getParameter(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.name()).isEqualTo(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.required()).isFalse();
        assertThat(valueEvaluatorType.multiple()).isFalse();
        assertThat(valueEvaluatorType.mutable()).isTrue();
        assertThat(valueEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(valueEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter valueSchemaPath = configSpec.getParameter(RECORD_VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaPath.name()).isEqualTo(RECORD_VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaPath.required()).isFalse();
        assertThat(valueSchemaPath.multiple()).isFalse();
        assertThat(valueSchemaPath.mutable()).isTrue();
        assertThat(valueSchemaPath.defaultValue()).isNull();
        assertThat(valueSchemaPath.type()).isEqualTo(ConfType.FILE);

        ConfParameter schemaRegistryEnabledForValue =
                configSpec.getParameter(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForValue.name())
                .isEqualTo(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForValue.required()).isFalse();
        assertThat(schemaRegistryEnabledForValue.multiple()).isFalse();
        assertThat(schemaRegistryEnabledForValue.mutable()).isTrue();
        assertThat(schemaRegistryEnabledForValue.defaultValue()).isEqualTo("false");
        assertThat(schemaRegistryEnabledForValue.type()).isEqualTo(ConfType.BOOL);

        ConfParameter keyKvpSeparator =
                configSpec.getParameter(RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(keyKvpSeparator.name()).isEqualTo(RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(keyKvpSeparator.required()).isFalse();
        assertThat(keyKvpSeparator.multiple()).isFalse();
        assertThat(keyKvpSeparator.mutable()).isTrue();
        assertThat(keyKvpSeparator.defaultValue()).isEqualTo(",");
        assertThat(keyKvpSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter keyKvpKeyValueSeparator =
                configSpec.getParameter(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(keyKvpKeyValueSeparator.name())
                .isEqualTo(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(keyKvpKeyValueSeparator.required()).isFalse();
        assertThat(keyKvpKeyValueSeparator.multiple()).isFalse();
        assertThat(keyKvpKeyValueSeparator.mutable()).isTrue();
        assertThat(keyKvpKeyValueSeparator.defaultValue()).isEqualTo("=");
        assertThat(keyKvpKeyValueSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter valueKvpSeparator =
                configSpec.getParameter(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(valueKvpSeparator.name()).isEqualTo(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(valueKvpSeparator.required()).isFalse();
        assertThat(valueKvpSeparator.multiple()).isFalse();
        assertThat(valueKvpSeparator.mutable()).isTrue();
        assertThat(valueKvpSeparator.defaultValue()).isEqualTo(",");
        assertThat(valueKvpSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter valueKvpKeyValueSeparator =
                configSpec.getParameter(RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(valueKvpKeyValueSeparator.name())
                .isEqualTo(RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(valueKvpKeyValueSeparator.required()).isFalse();
        assertThat(valueKvpKeyValueSeparator.multiple()).isFalse();
        assertThat(valueKvpKeyValueSeparator.mutable()).isTrue();
        assertThat(valueKvpKeyValueSeparator.defaultValue()).isEqualTo("=");
        assertThat(valueKvpKeyValueSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter keyEvaluatorProtobufMessageType =
                configSpec.getParameter(RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(keyEvaluatorProtobufMessageType.name())
                .isEqualTo(RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(keyEvaluatorProtobufMessageType.required()).isFalse();
        assertThat(keyEvaluatorProtobufMessageType.multiple()).isFalse();
        assertThat(keyEvaluatorProtobufMessageType.mutable()).isTrue();
        assertThat(keyEvaluatorProtobufMessageType.defaultValue()).isNull();
        assertThat(keyEvaluatorProtobufMessageType.type()).isEqualTo(ConfType.TEXT);

        ConfParameter valueEvaluatorProtobufMessageType =
                configSpec.getParameter(RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(valueEvaluatorProtobufMessageType.name())
                .isEqualTo(RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(valueEvaluatorProtobufMessageType.required()).isFalse();
        assertThat(valueEvaluatorProtobufMessageType.multiple()).isFalse();
        assertThat(valueEvaluatorProtobufMessageType.mutable()).isTrue();
        assertThat(valueEvaluatorProtobufMessageType.defaultValue()).isNull();
        assertThat(valueEvaluatorProtobufMessageType.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemInfoName = configSpec.getParameter(ITEM_INFO_NAME);
        assertThat(itemInfoName.name()).isEqualTo(ITEM_INFO_NAME);
        assertThat(itemInfoName.required()).isFalse();
        assertThat(itemInfoName.multiple()).isFalse();
        assertThat(itemInfoName.mutable()).isTrue();
        assertThat(itemInfoName.defaultValue()).isEqualTo("INFO");
        assertThat(itemInfoName.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemInfoField = configSpec.getParameter(ITEM_INFO_FIELD);
        assertThat(itemInfoField.name()).isEqualTo(ITEM_INFO_FIELD);
        assertThat(itemInfoField.required()).isFalse();
        assertThat(itemInfoField.multiple()).isFalse();
        assertThat(itemInfoField.mutable()).isTrue();
        assertThat(itemInfoField.defaultValue()).isEqualTo("MSG");
        assertThat(itemInfoField.type()).isEqualTo(ConfType.TEXT);

        ConfParameter recordExtractionErrorHandling =
                configSpec.getParameter(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY);
        assertThat(recordExtractionErrorHandling.name())
                .isEqualTo(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY);
        assertThat(recordExtractionErrorHandling.required()).isFalse();
        assertThat(recordExtractionErrorHandling.multiple()).isFalse();
        assertThat(recordExtractionErrorHandling.mutable()).isTrue();
        assertThat(recordExtractionErrorHandling.defaultValue()).isEqualTo("IGNORE_AND_CONTINUE");
        assertThat(recordExtractionErrorHandling.type()).isEqualTo(ConfType.ERROR_STRATEGY);

        ConfParameter recordConsumeAtConnectorStartUp =
                configSpec.getParameter(RECORD_CONSUME_AT_CONNECTOR_STARTUP_ENABLE);
        assertThat(recordConsumeAtConnectorStartUp.name())
                .isEqualTo(RECORD_CONSUME_AT_CONNECTOR_STARTUP_ENABLE);
        assertThat(recordConsumeAtConnectorStartUp.required()).isFalse();
        assertThat(recordConsumeAtConnectorStartUp.multiple()).isFalse();
        assertThat(recordConsumeAtConnectorStartUp.mutable()).isTrue();
        assertThat(recordConsumeAtConnectorStartUp.defaultValue()).isEqualTo("false");
        assertThat(recordConsumeAtConnectorStartUp.type()).isEqualTo(ConfType.BOOL);

        ConfParameter recordConsumeWithOrderStrategy =
                configSpec.getParameter(RECORD_CONSUME_WITH_ORDER_STRATEGY);
        assertThat(recordConsumeWithOrderStrategy.name())
                .isEqualTo(RECORD_CONSUME_WITH_ORDER_STRATEGY);
        assertThat(recordConsumeWithOrderStrategy.required()).isFalse();
        assertThat(recordConsumeWithOrderStrategy.multiple()).isFalse();
        assertThat(recordConsumeWithOrderStrategy.mutable()).isTrue();
        assertThat(recordConsumeWithOrderStrategy.defaultValue()).isEqualTo("ORDER_BY_PARTITION");
        assertThat(recordConsumeWithOrderStrategy.type()).isEqualTo(ConfType.ORDER_STRATEGY);

        ConfParameter recordConsumeWithThreadsNumber =
                configSpec.getParameter(RECORD_CONSUME_WITH_NUM_THREADS);
        assertThat(recordConsumeWithThreadsNumber.name())
                .isEqualTo(RECORD_CONSUME_WITH_NUM_THREADS);
        assertThat(recordConsumeWithThreadsNumber.required()).isFalse();
        assertThat(recordConsumeWithThreadsNumber.multiple()).isFalse();
        assertThat(recordConsumeWithThreadsNumber.mutable()).isTrue();
        assertThat(recordConsumeWithThreadsNumber.defaultValue()).isEqualTo("1");
        assertThat(recordConsumeWithThreadsNumber.type()).isEqualTo(ConfType.THREADS);

        ConfParameter enableAutoCommit =
                configSpec.getParameter(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.name()).isEqualTo(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.required()).isFalse();
        assertThat(enableAutoCommit.multiple()).isFalse();
        assertThat(enableAutoCommit.mutable()).isFalse();
        assertThat(enableAutoCommit.defaultValue()).isEqualTo("false");
        assertThat(enableAutoCommit.type()).isEqualTo(ConfType.BOOL);

        ConfParameter encryptionEnabled = configSpec.getParameter(ENCRYPTION_ENABLE);
        assertThat(encryptionEnabled.name()).isEqualTo(ENCRYPTION_ENABLE);
        assertThat(encryptionEnabled.required()).isFalse();
        assertThat(encryptionEnabled.multiple()).isFalse();
        assertThat(encryptionEnabled.mutable()).isTrue();
        assertThat(encryptionEnabled.defaultValue()).isEqualTo("false");
        assertThat(encryptionEnabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter authenticationEnabled = configSpec.getParameter(AUTHENTICATION_ENABLE);
        assertThat(authenticationEnabled.name()).isEqualTo(AUTHENTICATION_ENABLE);
        assertThat(authenticationEnabled.required()).isFalse();
        assertThat(authenticationEnabled.multiple()).isFalse();
        assertThat(authenticationEnabled.mutable()).isTrue();
        assertThat(authenticationEnabled.defaultValue()).isEqualTo("false");
        assertThat(authenticationEnabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter consumeEventsFrom = configSpec.getParameter(RECORD_CONSUME_FROM);
        assertThat(consumeEventsFrom.name()).isEqualTo(RECORD_CONSUME_FROM);
        assertThat(consumeEventsFrom.required()).isFalse();
        assertThat(consumeEventsFrom.multiple()).isFalse();
        assertThat(consumeEventsFrom.mutable()).isTrue();
        assertThat(consumeEventsFrom.defaultValue()).isEqualTo("LATEST");
        assertThat(consumeEventsFrom.type()).isEqualTo(ConfType.CONSUME_FROM);

        ConfParameter clientId = configSpec.getParameter(CONSUMER_CLIENT_ID);
        assertThat(clientId.name()).isEqualTo(CONSUMER_CLIENT_ID);
        assertThat(clientId.required()).isFalse();
        assertThat(clientId.multiple()).isFalse();
        assertThat(clientId.mutable()).isFalse();
        assertThat(clientId.defaultValue()).isEqualTo("");
        assertThat(clientId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter enableAutoCommitConfig =
                configSpec.getParameter(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommitConfig.name()).isEqualTo(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommitConfig.required()).isFalse();
        assertThat(enableAutoCommitConfig.multiple()).isFalse();
        assertThat(enableAutoCommitConfig.mutable()).isFalse();
        assertThat(enableAutoCommitConfig.defaultValue()).isEqualTo("false");
        assertThat(enableAutoCommitConfig.type()).isEqualTo(ConfType.BOOL);

        ConfParameter reconnectBackoffMaxMs =
                configSpec.getParameter(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        assertThat(reconnectBackoffMaxMs.name())
                .isEqualTo(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        assertThat(reconnectBackoffMaxMs.required()).isFalse();
        assertThat(reconnectBackoffMaxMs.multiple()).isFalse();
        assertThat(reconnectBackoffMaxMs.mutable()).isTrue();
        assertThat(reconnectBackoffMaxMs.defaultValue()).isNull();
        assertThat(reconnectBackoffMaxMs.type()).isEqualTo(ConfType.INT);

        ConfParameter reconnectBackoffMs =
                configSpec.getParameter(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG);
        assertThat(reconnectBackoffMs.name()).isEqualTo(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG);
        assertThat(reconnectBackoffMs.required()).isFalse();
        assertThat(reconnectBackoffMs.multiple()).isFalse();
        assertThat(reconnectBackoffMs.mutable()).isTrue();
        assertThat(reconnectBackoffMs.defaultValue()).isNull();
        assertThat(reconnectBackoffMs.type()).isEqualTo(ConfType.INT);

        ConfParameter fetchMinBytes = configSpec.getParameter(CONSUMER_FETCH_MIN_BYTES_CONFIG);
        assertThat(fetchMinBytes.name()).isEqualTo(CONSUMER_FETCH_MIN_BYTES_CONFIG);
        assertThat(fetchMinBytes.required()).isFalse();
        assertThat(fetchMinBytes.multiple()).isFalse();
        assertThat(fetchMinBytes.mutable()).isTrue();
        assertThat(fetchMinBytes.defaultValue()).isNull();
        assertThat(fetchMinBytes.type()).isEqualTo(ConfType.INT);

        ConfParameter fetchMaxBytes = configSpec.getParameter(CONSUMER_FETCH_MAX_BYTES_CONFIG);
        assertThat(fetchMaxBytes.name()).isEqualTo(CONSUMER_FETCH_MAX_BYTES_CONFIG);
        assertThat(fetchMaxBytes.required()).isFalse();
        assertThat(fetchMaxBytes.multiple()).isFalse();
        assertThat(fetchMaxBytes.mutable()).isTrue();
        assertThat(fetchMaxBytes.defaultValue()).isNull();
        assertThat(fetchMaxBytes.type()).isEqualTo(ConfType.INT);

        ConfParameter fetchMaxWaitMs = configSpec.getParameter(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG);
        assertThat(fetchMaxWaitMs.name()).isEqualTo(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG);
        assertThat(fetchMaxWaitMs.required()).isFalse();
        assertThat(fetchMaxWaitMs.multiple()).isFalse();
        assertThat(fetchMaxWaitMs.mutable()).isTrue();
        assertThat(fetchMaxWaitMs.defaultValue()).isNull();
        assertThat(fetchMaxWaitMs.type()).isEqualTo(ConfType.INT);

        ConfParameter maxPollRecords = configSpec.getParameter(RECORD_CONSUME_MAX_POLL_RECORDS);
        assertThat(maxPollRecords.name()).isEqualTo(RECORD_CONSUME_MAX_POLL_RECORDS);
        assertThat(maxPollRecords.required()).isFalse();
        assertThat(maxPollRecords.multiple()).isFalse();
        assertThat(maxPollRecords.mutable()).isTrue();
        assertThat(maxPollRecords.defaultValue()).isEqualTo("500");
        assertThat(maxPollRecords.type()).isEqualTo(ConfType.POSITIVE_INT);

        ConfParameter heartBeatIntervalMs = configSpec.getParameter(CONSUMER_HEARTBEAT_INTERVAL_MS);
        assertThat(heartBeatIntervalMs.name()).isEqualTo(CONSUMER_HEARTBEAT_INTERVAL_MS);
        assertThat(heartBeatIntervalMs.required()).isFalse();
        assertThat(heartBeatIntervalMs.multiple()).isFalse();
        assertThat(heartBeatIntervalMs.mutable()).isTrue();
        assertThat(heartBeatIntervalMs.defaultValue()).isNull();
        assertThat(heartBeatIntervalMs.type()).isEqualTo(ConfType.INT);

        ConfParameter sessionTimeoutMs = configSpec.getParameter(CONSUMER_SESSION_TIMEOUT_MS);
        assertThat(sessionTimeoutMs.name()).isEqualTo(CONSUMER_SESSION_TIMEOUT_MS);
        assertThat(sessionTimeoutMs.required()).isFalse();
        assertThat(sessionTimeoutMs.multiple()).isFalse();
        assertThat(sessionTimeoutMs.mutable()).isTrue();
        assertThat(sessionTimeoutMs.defaultValue()).isNull();
        assertThat(sessionTimeoutMs.type()).isEqualTo(ConfType.INT);

        ConfParameter maxPollIntervalMs = configSpec.getParameter(CONSUMER_MAX_POLL_INTERVAL_MS);
        assertThat(maxPollIntervalMs.name()).isEqualTo(CONSUMER_MAX_POLL_INTERVAL_MS);
        assertThat(maxPollIntervalMs.required()).isFalse();
        assertThat(maxPollIntervalMs.multiple()).isFalse();
        assertThat(maxPollIntervalMs.mutable()).isFalse();
        assertThat(maxPollIntervalMs.defaultValue()).isEqualTo("5000");
        assertThat(maxPollIntervalMs.type()).isEqualTo(ConfType.INT);

        ConfParameter metadataMaxAge = configSpec.getParameter(CONSUMER_METADATA_MAX_AGE_CONFIG);
        assertThat(metadataMaxAge.name()).isEqualTo(CONSUMER_METADATA_MAX_AGE_CONFIG);
        assertThat(metadataMaxAge.required()).isFalse();
        assertThat(metadataMaxAge.multiple()).isFalse();
        assertThat(metadataMaxAge.mutable()).isFalse();
        assertThat(metadataMaxAge.defaultValue()).isEqualTo("250");
        assertThat(metadataMaxAge.type()).isEqualTo(ConfType.INT);

        ConfParameter requestTimeoutMs =
                configSpec.getParameter(CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        assertThat(requestTimeoutMs.name()).isEqualTo(CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        assertThat(requestTimeoutMs.required()).isFalse();
        assertThat(requestTimeoutMs.multiple()).isFalse();
        assertThat(requestTimeoutMs.mutable()).isFalse();
        assertThat(requestTimeoutMs.defaultValue()).isEqualTo("30000");
        assertThat(requestTimeoutMs.type()).isEqualTo(ConfType.INT);
    }

    private Map<String, String> standardParameters() {
        Map<String, String> standardParams = new HashMap<>();
        standardParams.put(BOOTSTRAP_SERVERS, "server:8080,server:8081");
        standardParams.put(RECORD_VALUE_EVALUATOR_TYPE, "STRING");
        // standardParams.put(RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
        // valueSchemaFile.getFileName().toString());
        standardParams.put(RECORD_KEY_EVALUATOR_TYPE, "JSON");
        // standardParams.put(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH,keySchemaFile.getFileName().toString());
        standardParams.put(ConnectorConfig.ITEM_INFO_NAME, "INFO_ITEM");
        standardParams.put(ConnectorConfig.ITEM_INFO_FIELD, "INFO_FIELD");
        standardParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        standardParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
        standardParams.put(ConnectorConfig.CONSUMER_CLIENT_ID, "a.client.id"); // Unmodifiable
        standardParams.put(ConnectorConfig.CONSUMER_FETCH_MAX_BYTES_CONFIG, "100");
        standardParams.put(ConnectorConfig.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG, "200");
        standardParams.put(ConnectorConfig.CONSUMER_FETCH_MIN_BYTES_CONFIG, "300");
        standardParams.put(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG, "400");
        standardParams.put(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG, "500");
        standardParams.put(CONSUMER_HEARTBEAT_INTERVAL_MS, "600");
        standardParams.put(ConnectorConfig.RECORD_CONSUME_MAX_POLL_RECORDS, "700");
        standardParams.put(CONSUMER_SESSION_TIMEOUT_MS, "800");
        standardParams.put(CONSUMER_MAX_POLL_INTERVAL_MS, "2000"); // Unmodifiable
        standardParams.put(CONSUMER_METADATA_MAX_AGE_CONFIG, "250"); // Unmodifiable
        standardParams.put(
                ConnectorConfig.CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG, "1000"); // Unmodifiable
        standardParams.put(CONSUMER_REQUEST_TIMEOUT_MS_CONFIG, "15000"); // Unmodifiable
        standardParams.put("item-template.template1", "template1-#{v=VALUE}");
        standardParams.put("item-template.template2", "template2-#{v=OFFSET}");
        standardParams.put("map.topic1.to", "template1");
        standardParams.put("map.topic2.to", "template2");
        standardParams.put("field.fieldName1", "#{VALUE.bar}");
        return standardParams;
    }

    private Map<String, String> encryptionParameters() {
        Map<String, String> encryptionParams = new HashMap<>();
        encryptionParams.put(ENCRYPTION_ENABLE, "true");
        return encryptionParams;
    }

    private Map<String, String> keystoreParameters() {
        Map<String, String> keystoreParams = new HashMap<>();
        keystoreParams.put(EncryptionConfigs.ENABLE_MTLS, "true");
        keystoreParams.put(EncryptionConfigs.KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        return keystoreParams;
    }

    private Map<String, String> authenticationParameters() {
        Map<String, String> authParams = new HashMap<>();
        authParams.put(AUTHENTICATION_ENABLE, "true");
        authParams.put(USERNAME, "sasl-username");
        authParams.put(PASSWORD, "sasl-password");
        return authParams;
    }

    @Test
    public void shouldSpecifyRequiredParams() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> new ConnectorConfig(Collections.emptyMap()));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [%s]".formatted(ADAPTERS_CONF_ID));

        Map<String, String> params = new HashMap<>();

        params.put(ADAPTERS_CONF_ID, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ADAPTERS_CONF_ID));

        params.put(ADAPTERS_CONF_ID, "adapters_conf_id");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [%s]".formatted(ADAPTER_DIR));

        params.put(ADAPTER_DIR, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ADAPTER_DIR));

        params.put(ADAPTER_DIR, "non-existing-directory");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Not found directory [non-existing-directory] specified in [%s]"
                                .formatted(ADAPTER_DIR));

        params.put(ADAPTER_DIR, adapterDir.toString());
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [%s]".formatted(DATA_ADAPTER_NAME));

        params.put(DATA_ADAPTER_NAME, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(DATA_ADAPTER_NAME));

        params.put(DATA_ADAPTER_NAME, "data_provider_name");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [%s]".formatted(BOOTSTRAP_SERVERS));

        params.put(BOOTSTRAP_SERVERS, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(BOOTSTRAP_SERVERS));

        // Trailing "," not allowed
        params.put(BOOTSTRAP_SERVERS, "server:8080,");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(BOOTSTRAP_SERVERS));

        params.put(BOOTSTRAP_SERVERS, "server:8080");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce).hasMessageThat().isEqualTo("Specify at least one parameter [map.<...>.to]");

        params.put("map.to", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce).hasMessageThat().isEqualTo("Specify a valid parameter [map.<...>.to]");
        params.remove("map.to");

        params.put("map.topic.to", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [map.topic.to]");

        // Trailing "," not allowed
        params.put("map.topic.to", "item1,");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [map.topic.to]");

        params.put("map.topic.to", "aTemplate");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce).hasMessageThat().isEqualTo("Specify at least one parameter [field.<...>]");
        params.put("field.", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce).hasMessageThat().isEqualTo("Specify a valid parameter [field.<...>]");
        params.remove("field.");

        params.put("field.field1", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [field.field1]");

        params.put("field.field1", "#{}");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Got the following error while evaluating the field [field1] containing the expression [#{}]: <Invalid expression>");

        params.put("field.field1", "#{VALUE}");
        assertDoesNotThrow(() -> new ConnectorConfig(params));
    }

    @Test
    public void shouldRetrieveConfiguration() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        Map<String, String> configuration = config.configuration();
        assertThat(configuration).isNotEmpty();
    }

    @Test
    public void shouldRetrieveBaseConsumerProperties() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        Properties baseConsumerProps = config.baseConsumerProps();
        assertThat(baseConsumerProps)
                .containsAtLeast(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        "server:8080,server:8081",
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        "",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "latest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        "false",
                        ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                        "100",
                        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                        "200",
                        ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                        "300",
                        ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        "400",
                        ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG,
                        "500",
                        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                        "600",
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                        "700",
                        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                        "800",
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                        "5000",
                        ConsumerConfig.METADATA_MAX_AGE_CONFIG,
                        "250",
                        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                        "60000",
                        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                        "30000");
        assertThat(baseConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                .startsWith("KAFKA-CONNECTOR-");
    }

    static Stream<String> confluentCloudHostList() {
        return Stream.of(
                "abc-57rr02.mycloudrovider1.confluent.cloud:9092",
                "def-437seq1.mycloudrovider2.confluent.cloud:9092,lopc-32wwg15.mycloudrovider2.confluent.cloud:9092");
    }

    @ParameterizedTest
    @MethodSource("confluentCloudHostList")
    public void shouldRetrieveLightstreamerClientIdWhenConnectedToConfluentClod(String hostList) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostList);
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        Properties baseConsumerProps = config.baseConsumerProps();
        assertThat(baseConsumerProps)
                .containsAtLeast(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        hostList,
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        LIGHTSTREAMER_CLIENT_ID);
    }

    static Stream<String> partialConfluentCloudHostList() {
        return Stream.of(
                "def-437seq1.mycloudrovider2.my.com:9092,lopc-32wwg15.mycloudrovider2.confluent.cloud1:9092");
    }

    @ParameterizedTest
    @MethodSource("partialConfluentCloudHostList")
    public void shouldNonRetrieveLightstreamerClientIdWhenNotAllHostConnectedToConfluentClod(
            String hostList) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostList);
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        Properties baseConsumerProps = config.baseConsumerProps();
        assertThat(baseConsumerProps)
                .containsAtLeast(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        hostList,
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        "");
    }

    @Test
    public void shouldExtendBaseConsumerProperties() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        Map<String, ?> extendedProps = config.extendsConsumerProps(Map.of("new.key", "new.value"));
        assertThat(extendedProps)
                .containsAtLeast(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        "server:8080,server:8081",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "latest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        "false",
                        "new.key",
                        "new.value");
        assertThat(extendedProps.get(ConsumerConfig.GROUP_ID_CONFIG).toString())
                .startsWith("KAFKA-CONNECTOR-");
    }

    @Test
    public void shouldNotModifyEnableAutoCommitConfig() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.getBoolean(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG)).isFalse();
    }

    @Test
    public void shouldGetText() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.getMetadataAdapterName()).isEqualTo("KAFKA");
        assertThat(config.getAdapterName()).isEqualTo("CONNECTOR");
        assertThat(config.getItemInfoName()).isEqualTo("INFO_ITEM");
        assertThat(config.getItemInfoField()).isEqualTo("INFO_FIELD");

        String groupId = config.getText(GROUP_ID);
        assertThat(groupId).startsWith("KAFKA-CONNECTOR-");
        assertThat(groupId.length()).isGreaterThan("KAFKA-CONNECTOR-".length());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "AVRO",
                "PROTOBUF",
                "STRING",
                "KVP",
                "JSON",
                "INTEGER",
                "SHORT",
                "FLOAT",
                "LONG",
                "UUID",
                "DOUBLE",
                "BOOLEAN",
                "BYTES",
                "BYTE_ARRAY",
                "BYTE_BUFFER"
            })
    public void shouldGetRecordEvaluatorTypes(String type) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_KEY_EVALUATOR_TYPE, type);
        updatedConfig.put(RECORD_VALUE_EVALUATOR_TYPE, type);
        if (List.of("AVRO", "PROTOBUF").contains(type)) {
            updatedConfig.put(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
            updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
            updatedConfig.put(SchemaRegistryConfigs.URL, "http://localhost:8081");
        }
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getValueEvaluator()).isEqualTo(EvaluatorType.valueOf(type));
        assertThat(config.getKeyEvaluator()).isEqualTo(EvaluatorType.valueOf(type));
    }

    @Test
    public void shouldFailDueToInvalidEvaluatorType() {
        Map<String, String> keys =
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        "[record.key.evaluator.type]",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        "[record.value.evaluator.type]");
        for (Map.Entry<String, String> entry : keys.entrySet()) {
            Map<String, String> updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.put(entry.getKey(), "invalidType");
            ConfigException ce =
                    assertThrows(
                            ConfigException.class,
                            () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo("Specify a valid value for parameter " + entry.getValue());
        }
    }

    @Test
    public void shouldFailDueToInvalidSchemaPath() {
        Map<String, String> keys =
                Map.of(
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "[key.evaluator.schema.path]",
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "[value.evaluator.schema.path]");
        for (Map.Entry<String, String> entry : keys.entrySet()) {
            Map<String, String> updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.put(entry.getKey(), "invalidSchemaPath");
            ConfigException ce =
                    assertThrows(
                            ConfigException.class,
                            () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Not found file [%s/invalidSchemaPath] specified in [%s]"
                                    .formatted(adapterDir, entry.getKey()));
        }
    }

    @Test
    public void shouldSpecifyRequiredParamsForAvro() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(RECORD_KEY_EVALUATOR_TYPE, "AVRO")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid file path for [record.key.evaluator.schema.path] or set [record.key.evaluator.schema.registry.enable] to true");

        assertDoesNotThrow(
                () ->
                        ConnectorConfigProvider.minimalWith(
                                adapterDir.toString(),
                                Map.of(
                                        RECORD_KEY_EVALUATOR_TYPE,
                                        "AVRO",
                                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                        avroKeySchemaFile.getFileName().toString())));
        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                RECORD_KEY_EVALUATOR_TYPE,
                                                "AVRO",
                                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                                "true")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [schema.registry.url]");

        assertDoesNotThrow(
                () ->
                        ConnectorConfigProvider.minimalWith(
                                adapterDir.toString(),
                                Map.of(
                                        RECORD_KEY_EVALUATOR_TYPE,
                                        "AVRO",
                                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                        "true",
                                        SchemaRegistryConfigs.URL,
                                        "http://localhost:8081")));

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, "AVRO")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid file path for [record.value.evaluator.schema.path] or set [record.value.evaluator.schema.registry.enable] to true");

        assertDoesNotThrow(
                () ->
                        ConnectorConfigProvider.minimalWith(
                                adapterDir.toString(),
                                Map.of(
                                        RECORD_VALUE_EVALUATOR_TYPE,
                                        "AVRO",
                                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                        avroValueSchemaFile.getFileName().toString())));

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                RECORD_VALUE_EVALUATOR_TYPE,
                                                "AVRO",
                                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                                "true")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [schema.registry.url]");

        assertDoesNotThrow(
                () ->
                        ConnectorConfigProvider.minimalWith(
                                adapterDir.toString(),
                                Map.of(
                                        RECORD_VALUE_EVALUATOR_TYPE,
                                        "AVRO",
                                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                        "true",
                                        SchemaRegistryConfigs.URL,
                                        "http://localhost:8081")));
    }

    @Test
    public void shouldSpecifyRequiredParamsForProtobuf() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(RECORD_KEY_EVALUATOR_TYPE, "PROTOBUF")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid file path for [record.key.evaluator.schema.path] or set [record.key.evaluator.schema.registry.enable] to true");

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                RECORD_KEY_EVALUATOR_TYPE,
                                                "PROTOBUF",
                                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                                protoKeySchemaFile.getFileName().toString())));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [record.key.evaluator.protobuf.message.type]");

        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_KEY_EVALUATOR_TYPE,
                                "PROTOBUF",
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                protoKeySchemaFile.getFileName().toString(),
                                RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "keyMessage"));
        assertThat(config.getProtobufKeyMessageType()).isEqualTo("keyMessage");

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                RECORD_KEY_EVALUATOR_TYPE,
                                                "PROTOBUF",
                                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                                "true")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [schema.registry.url]");

        assertDoesNotThrow(
                () ->
                        ConnectorConfigProvider.minimalWith(
                                adapterDir.toString(),
                                Map.of(
                                        RECORD_KEY_EVALUATOR_TYPE,
                                        "PROTOBUF",
                                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                        "true",
                                        SchemaRegistryConfigs.URL,
                                        "http://localhost:8081")));

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, "PROTOBUF")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid file path for [record.value.evaluator.schema.path] or set [record.value.evaluator.schema.registry.enable] to true");

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                RECORD_VALUE_EVALUATOR_TYPE,
                                                "PROTOBUF",
                                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                                protoValueSchemaFile.getFileName().toString())));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [record.value.evaluator.protobuf.message.type]");

        config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                "PROTOBUF",
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                protoValueSchemaFile.getFileName().toString(),
                                RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE,
                                "valueMessage"));
        assertThat(config.getProtobufValueMessageType()).isEqualTo("valueMessage");

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                RECORD_VALUE_EVALUATOR_TYPE,
                                                "PROTOBUF",
                                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                                "true")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [schema.registry.url]");

        assertDoesNotThrow(
                () ->
                        ConnectorConfigProvider.minimalWith(
                                adapterDir.toString(),
                                Map.of(
                                        RECORD_VALUE_EVALUATOR_TYPE,
                                        "PROTOBUF",
                                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                        "true",
                                        SchemaRegistryConfigs.URL,
                                        "http://localhost:8081")));
    }

    @Test
    public void shouldGetKvpPairsSeparator() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getKeyKvpPairsSeparator()).isEqualTo(',');
        assertThat(config.getValueKvpPairsSeparator()).isEqualTo(',');

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR, ";");
        updatedConfig.put(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR, "|");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getKeyKvpPairsSeparator()).isEqualTo(';');
        assertThat(config.getValueKvpPairsSeparator()).isEqualTo('|');
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"==", ";;"})
    public void shouldFailDueToInvalidKvpPairsSeparator(String delimiter) {
        Map<String, String> configs1 = new HashMap<>();
        configs1.put(ConnectorConfig.RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR, delimiter);

        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs1));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.key.evaluator.kvp.pairs.separator]");

        Map<String, String> configs2 = new HashMap<>();
        configs2.put(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR, delimiter);

        ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs2));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.value.evaluator.kvp.pairs.separator]");
    }

    @Test
    public void shouldGetKvpValueSeparator() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getKeyKvpKeyValueSeparator()).isEqualTo('=');
        assertThat(config.getValueKvpKeyValueSeparator()).isEqualTo('=');

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, "@");
        updatedConfig.put(RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, "|");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getKeyKvpKeyValueSeparator()).isEqualTo('@');
        assertThat(config.getValueKvpKeyValueSeparator()).isEqualTo('|');
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"==", ";;"})
    public void shouldFailDueToInvalidKvpSeparator(String delimiter) {
        Map<String, String> configs1 = new HashMap<>();
        configs1.put(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, delimiter);

        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs1));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.key.evaluator.kvp.key-value.separator]");

        Map<String, String> configs2 = new HashMap<>();
        configs2.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, delimiter);

        ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs2));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.value.evaluator.kvp.key-value.separator]");
    }

    @Test
    public void shouldGeCommandModeEnforce() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.isCommandEnforceEnabled()).isFalse();

        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(FIELDS_EVALUATE_AS_COMMAND_ENABLE, "false"));
        assertThat(config.isCommandEnforceEnabled()).isFalse();

        // Checks value "true"
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                "true",
                                "field.key",
                                "#{KEY}",
                                "field.command",
                                "#{VALUE}"));
        assertThat(config.isCommandEnforceEnabled()).isTrue();

        // Checks invalid values
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(FIELDS_EVALUATE_AS_COMMAND_ENABLE, "invalid")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [fields.evaluate.as.command.enable]");

        // Check that field.key is set
        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(FIELDS_EVALUATE_AS_COMMAND_ENABLE, "true")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Command mode requires a key field. Parameter [field.key] must be set");

        // Check that field.command is set
        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(
                                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                                "true",
                                                "field.key",
                                                "#{KEY}")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Command mode requires a command field. Parameter [field.command] must be set");

        // Requires that exactly one consumer thread is set
        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(
                                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                                "true",
                                                RECORD_CONSUME_WITH_NUM_THREADS,
                                                "2")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Command mode requires exactly one consumer thread. Parameter [record.consume.with.num.threads] must be set to [1]");
    }

    @Test
    public void shouldGetAutoCommandMode() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.isAutoCommandModeEnabled()).isFalse();

        // Checks value "false"
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(FIELDS_AUTO_COMMAND_MODE_ENABLE, "false"));
        assertThat(config.isAutoCommandModeEnabled()).isFalse();

        // Checks value "true"
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(FIELDS_AUTO_COMMAND_MODE_ENABLE, "true", "field.key", "#{KEY}"));
        assertThat(config.isAutoCommandModeEnabled()).isTrue();

        // Check that field.key is set
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        Map.of(FIELDS_AUTO_COMMAND_MODE_ENABLE, "true")));
        assertThat(ce.getMessage())
                .isEqualTo("Command mode requires a key field. Parameter [field.key] must be set");
    }

    @Test
    public void shouldGetCommandModeStrategy() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();

        // Checks value "NONE"
        assertThat(config.getCommandModeStrategy()).isEqualTo(CommandModeStrategy.NONE);

        // Checks value "NONE" with both fields explicitly set to false
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                FIELDS_AUTO_COMMAND_MODE_ENABLE,
                                "false",
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                "false"));
        assertThat(config.getCommandModeStrategy()).isEqualTo(CommandModeStrategy.NONE);

        // Checks value "AUTO"
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(FIELDS_AUTO_COMMAND_MODE_ENABLE, "true", "field.key", "#{KEY}"));
        assertThat(config.getCommandModeStrategy()).isEqualTo(CommandModeStrategy.AUTO);

        // Checks value "AUTO" even if FIELDS_EVALUATE_AS_COMMAND_ENABLE is set to true
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                FIELDS_AUTO_COMMAND_MODE_ENABLE,
                                "true",
                                "field.key",
                                "#{KEY}",
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                "true"));
        assertThat(config.getCommandModeStrategy()).isEqualTo(CommandModeStrategy.AUTO);

        // Checks value "AUTO" with FIELDS_EVALUATE_AS_COMMAND_ENABLE explicitly set to false
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                FIELDS_AUTO_COMMAND_MODE_ENABLE,
                                "true",
                                "field.key",
                                "#{KEY}",
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                "false"));
        assertThat(config.getCommandModeStrategy()).isEqualTo(CommandModeStrategy.AUTO);

        // Checks value "ENFORCE"
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                "true",
                                "field.key",
                                "#{VALUE.aKey}",
                                "field.command",
                                "#{VALUE.aCommand}"));
        assertThat(config.getCommandModeStrategy()).isEqualTo(CommandModeStrategy.ENFORCE);

        // Checks value "ENFORCE" with FIELDS_TRANSFORM_TO_COMMAND_ENABLE explicitly set to false
        config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                FIELDS_AUTO_COMMAND_MODE_ENABLE,
                                "false",
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                "true",
                                "field.key",
                                "#{VALUE.aKey}",
                                "field.command",
                                "#{VALUE.aCommand}"));
        assertThat(config.getCommandModeStrategy()).isEqualTo(CommandModeStrategy.ENFORCE);
    }

    @Test
    public void shouldGetOverriddenGroupId() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(GROUP_ID, "group-id");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getText(GROUP_ID)).isEqualTo("group-id");
    }

    @Test
    void shouldGetTopicMappingWithOneReference() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("map.topic-test.to", "item-template.template1");
        ConnectorConfig cgg1 = ConnectorConfigProvider.minimalWith(updatedConfigs);

        List<TopicMappingConfig> topicMappings = cgg1.getTopicMappings();
        assertThat(topicMappings).hasSize(2);

        TopicMappingConfig tm1 = topicMappings.get(0);
        assertThat(tm1.topic()).isEqualTo("topic-test");
        assertThat(tm1.mappings()).containsExactly("item-template.template1");
    }

    @Test
    void shouldGetTopicMappingWithMoreReferences() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("map.topic-test.to", "item-template.template1,item1,item1,item2");
        ConnectorConfig cgg1 = ConnectorConfigProvider.minimalWith(updatedConfigs);

        List<TopicMappingConfig> topicMappings = cgg1.getTopicMappings();
        assertThat(topicMappings).hasSize(2);

        TopicMappingConfig tm1 = topicMappings.get(0);
        assertThat(tm1.topic()).isEqualTo("topic-test");
        assertThat(tm1.mappings()).containsExactly("item-template.template1", "item1", "item2");
    }

    @Test
    void shouldGetItemTemplateConfigs() {
        ConnectorConfig cgg1 = ConnectorConfigProvider.minimal();

        var templateConfig = cgg1.getItemTemplateConfigs();
        assertThat(templateConfig.templates()).isEmpty();

        ConnectorConfig cgg2 =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                "item-template.template1",
                                "item1-#{param1=VALUE.value1}",
                                "item-template.template2",
                                "item2-#{param2=VALUE.value2}"));

        var templateConfigs = cgg2.getItemTemplateConfigs();
        assertThat(templateConfigs.templates()).hasSize(2);

        TemplateExpression te1 = templateConfigs.getTemplateExpression("template1");
        assertThat(te1.prefix()).isEqualTo("item1");
        assertThat(te1.params())
                .containsExactly("param1", WrappedNoWildcardCheck("#{VALUE.value1}"));

        TemplateExpression te2 = templateConfigs.getTemplateExpression("template2");
        assertThat(te2.prefix()).isEqualTo("item2");
        assertThat(te2.params())
                .containsExactly("param2", WrappedNoWildcardCheck("#{VALUE.value2}"));
    }

    @Test
    public void shouldGetMapRegEx() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.isMapRegExEnabled()).isFalse();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.MAP_REG_EX_ENABLE, "true");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isMapRegExEnabled()).isTrue();
    }

    @Test
    public void shouldFailDueToInvalidMapRegExFlag() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ConnectorConfig.MAP_REG_EX_ENABLE, "t");

        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [map.regex.enable]");
    }

    @Test
    public void shouldGetFieldsSkipFailedMapping() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.isFieldsSkipFailedMappingEnabled()).isFalse();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.FIELDS_SKIP_FAILED_MAPPING_ENABLE, "true");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isFieldsSkipFailedMappingEnabled()).isTrue();
    }

    @Test
    public void shouldGetFieldsMapNonScalarValues() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.isFieldsMapNonScalarValuesEnabled()).isFalse();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.FIELDS_MAP_NON_SCALAR_VALUES_ENABLE, "true");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isFieldsMapNonScalarValuesEnabled()).isTrue();
    }

    @Test
    public void shouldFailDueToFieldsSkipFailedMapping() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ConnectorConfig.FIELDS_SKIP_FAILED_MAPPING_ENABLE, "t");

        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [fields.skip.failed.mapping.enable]");
    }

    @Test
    public void shouldFailDueToFieldsMapNonScalarValues() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ConnectorConfig.FIELDS_MAP_NON_SCALAR_VALUES_ENABLE, "t");

        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [fields.map.non.scalar.values.enable]");
    }

    @Test
    public void shouldFailDueToInvalidRegularExpressionInTopicMapping() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ConnectorConfig.MAP_REG_EX_ENABLE, "true");
        configs.put("map.topic_\\d.to", "item"); // Valid regular expression
        configs.put("map.\\k.to", "item"); // Invalid regular expression

        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid regular expression for parameter [map.\\k.to]");
    }

    @Test
    void shouldGetFieldConfigs() {
        ConnectorConfig cgg = ConnectorConfigProvider.minimal();
        FieldConfigs fieldConfigs = cgg.getFieldConfigs();
        assertThat(fieldConfigs.namedFieldsExpressions()).hasSize(1);
        assertThat(fieldConfigs.namedFieldsExpressions().get("fieldName1").toString())
                .isEqualTo("VALUE");
    }

    @Test
    public void shouldGetHostList() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getHostsList(BOOTSTRAP_SERVERS)).isEqualTo("server:8080,server:8081");
    }

    @Test
    public void shouldGetDefaultText() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getText(ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getText(DATA_ADAPTER_NAME)).isEqualTo("CONNECTOR");
        assertThat(config.getText(ITEM_INFO_NAME)).isEqualTo("INFO");
        assertThat(config.getText(ITEM_INFO_FIELD)).isEqualTo("MSG");
    }

    @Test
    public void shouldGetEnabled() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.isEnabled()).isTrue();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ENABLE, "false");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isEnabled()).isFalse();
    }

    @Test
    public void shouldOverrideConsumeEventsFrom() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_FROM, "EARLIEST");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordConsumeFrom()).isEqualTo(EARLIEST);
        assertThat(config.baseConsumerProps())
                .containsAtLeast(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void shouldGetEncryptionEnabled() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.isEncryptionEnabled()).isFalse();
    }

    @Test
    public void shouldGetAuthenticationEnabled() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.isAuthenticationEnabled()).isFalse();
    }

    @Test
    public void shouldGetDefaultEvaluator() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getKeyEvaluator()).isEqualTo(STRING);
        assertThat(config.getValueEvaluator()).isEqualTo(STRING);

        assertThat(config.isSchemaRegistryEnabledForKey()).isFalse();
        assertThat(config.isSchemaRegistryEnabledForValue()).isFalse();
        assertThat(config.isSchemaRegistryEnabled()).isFalse();
        assertThat(config.hasKeySchemaFile()).isFalse();
        assertThat(config.hasValueSchemaFile()).isFalse();
        assertThat(config.hasSchemaFile()).isFalse();
    }

    @Test
    public void shouldGetErrorStrategy() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getRecordExtractionErrorHandlingStrategy())
                .isEqualTo(IGNORE_AND_CONTINUE);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(
                RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, FORCE_UNSUBSCRIPTION.toString());
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordExtractionErrorHandlingStrategy())
                .isEqualTo(FORCE_UNSUBSCRIPTION);
    }

    @Test
    public void shouldFailDueToInvalidErrorStrategyType() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, "invalidType");
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter ["
                                + RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY
                                + "]");
    }

    @Test
    public void shouldGetRecordConsumeWithOrderStrategy() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getRecordConsumeWithOrderStrategy())
                .isEqualTo(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(
                RECORD_CONSUME_WITH_ORDER_STRATEGY,
                RecordConsumeWithOrderStrategy.ORDER_BY_KEY.toString());
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordConsumeWithOrderStrategy())
                .isEqualTo(RecordConsumeWithOrderStrategy.ORDER_BY_KEY);
    }

    @Test
    public void shouldFailDueToInvalidOrderStrategyType() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_ORDER_STRATEGY, "invalidType");
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter ["
                                + RECORD_CONSUME_WITH_ORDER_STRATEGY
                                + "]");
    }

    @Test
    public void shouldGetRecordConsumeAtConnectorStartUp() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.consumeAtStartup()).isEqualTo(false);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_AT_CONNECTOR_STARTUP_ENABLE, "true");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.consumeAtStartup()).isEqualTo(true);
    }

    @Test
    public void shouldFailDueToInvalidRecordConsumeAtConnectorStartUp() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_AT_CONNECTOR_STARTUP_ENABLE, "invalidType");
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter ["
                                + RECORD_CONSUME_AT_CONNECTOR_STARTUP_ENABLE
                                + "]");
    }

    @Test
    public void shouldGetRecordConsumeWithThreadsNumber() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getRecordConsumeWithNumThreads()).isEqualTo(1);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_NUM_THREADS, "10");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordConsumeWithNumThreads()).isEqualTo(10);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -2})
    public void shouldFailDueToInvalidRecordConsumeWithThreadsNumber(int invalidThreadsNumber) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_NUM_THREADS, String.valueOf(invalidThreadsNumber));
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter ["
                                + RECORD_CONSUME_WITH_NUM_THREADS
                                + "]");
    }

    @Test
    public void shouldGetOverriddenMaxPollRecords() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_CONSUME_MAX_POLL_RECORDS, "200");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.baseConsumerProps())
                .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc"})
    public void shouldFailDueToInvalidMaxPollRecords(String invalidMaxPollRecords) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_CONSUME_MAX_POLL_RECORDS, invalidMaxPollRecords);
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [record.consume.max.poll.records]");
    }

    @Test
    public void shouldGetDirectory() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getDirectory(ADAPTER_DIR)).isEqualTo(adapterDir.toString());
    }

    @Test
    public void shouldManageSchemaFiles() {
        ConnectorConfig configWithoutSchemas =
                ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(configWithoutSchemas.getFile(RECORD_KEY_EVALUATOR_SCHEMA_PATH)).isNull();
        assertThat(configWithoutSchemas.getFile(RECORD_VALUE_EVALUATOR_SCHEMA_PATH)).isNull();
        assertThat(configWithoutSchemas.hasKeySchemaFile()).isFalse();
        assertThat(configWithoutSchemas.hasValueSchemaFile()).isFalse();
        assertThat(configWithoutSchemas.hasSchemaFile()).isFalse();

        ConnectorConfig configWithKeySchema =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                avroKeySchemaFile.getFileName().toString()));

        assertThat(configWithKeySchema.getFile(RECORD_KEY_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(avroKeySchemaFile.toString());
        assertThat(configWithKeySchema.hasKeySchemaFile()).isTrue();
        assertThat(configWithKeySchema.hasValueSchemaFile()).isFalse();
        assertThat(configWithKeySchema.hasSchemaFile()).isTrue();

        ConnectorConfig configWithValueSchema =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                avroValueSchemaFile.getFileName().toString()));

        assertThat(configWithValueSchema.getFile(RECORD_VALUE_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(avroValueSchemaFile.toString());
        assertThat(configWithValueSchema.hasKeySchemaFile()).isFalse();
        assertThat(configWithValueSchema.hasValueSchemaFile()).isTrue();
        assertThat(configWithValueSchema.hasSchemaFile()).isTrue();

        ConnectorConfig configWithKeyAndValueSchemas =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
                        Map.of(
                                RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                                        avroKeySchemaFile.getFileName().toString(),
                                RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                                        avroValueSchemaFile.getFileName().toString()));

        assertThat(configWithKeyAndValueSchemas.getFile(RECORD_KEY_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(avroKeySchemaFile.toString());
        assertThat(configWithKeyAndValueSchemas.getFile(RECORD_VALUE_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(avroValueSchemaFile.toString());
        assertThat(configWithKeyAndValueSchemas.hasKeySchemaFile()).isTrue();
        assertThat(configWithKeyAndValueSchemas.hasValueSchemaFile()).isTrue();
        assertThat(configWithKeyAndValueSchemas.hasSchemaFile()).isTrue();
    }

    @Test
    public void shouldNoGetNonExistingNonRequiredInt() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getInt(CONSUMER_FETCH_MAX_BYTES_CONFIG)).isNull();
        assertThat(config.getInt(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG)).isNull();
        assertThat(config.getInt(CONSUMER_FETCH_MIN_BYTES_CONFIG)).isNull();
        assertThat(config.getInt(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG)).isNull();
        assertThat(config.getInt(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG)).isNull();
        assertThat(config.getInt(CONSUMER_HEARTBEAT_INTERVAL_MS)).isNull();
        assertThat(config.getInt(CONSUMER_SESSION_TIMEOUT_MS)).isNull();
    }

    @Test
    public void shouldNotAccessToEncryptionSettings() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        assertThat(config.isEncryptionEnabled()).isFalse();

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.isKeystoreEnabled(),
                        () -> config.enabledProtocols(),
                        () -> config.enabledProtocolsAsStr(),
                        () -> config.sslProtocol(),
                        () -> config.truststoreType(),
                        () -> config.truststorePath(),
                        () -> config.truststorePassword(),
                        () -> config.isHostNameVerificationEnabled(),
                        () -> config.cipherSuites(),
                        () -> config.cipherSuitesAsStr(),
                        () -> config.sslProvider());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo("Encryption is not enabled. Check parameter [encryption.enable]");
        }
    }

    @Test
    public void shouldSpecifyEncryptionParametersWhenRequired() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ENCRYPTION_ENABLE, "true");

        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PATH, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [encryption.truststore.path]");

        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Not found file ["
                                + adapterDir.toString()
                                + "/aFile] specified in [encryption.truststore.path]");

        updatedConfig.put(
                EncryptionConfigs.TRUSTSTORE_PATH, trustStoreFile.getFileName().toString());
    }

    @Test
    public void shouldGetDefaultEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isEncryptionEnabled()).isTrue();
        assertThat(config.enabledProtocols()).containsExactly(TLSv12, TLSv13);
        assertThat(config.enabledProtocolsAsStr()).isEqualTo("TLSv1.2,TLSv1.3");
        assertThat(config.sslProtocol().toString()).isEqualTo("TLSv1.3");
        assertThat(config.truststoreType().toString()).isEqualTo("JKS");
        assertThat(config.truststorePassword()).isNull();
        assertThat(config.truststorePath()).isNull();
        assertThat(config.isHostNameVerificationEnabled()).isFalse();
        assertThat(config.cipherSuites()).isEmpty();
        assertThat(config.cipherSuitesAsStr()).isNull();
        assertThat(config.sslProvider()).isNull();
        assertThat(config.isKeystoreEnabled()).isFalse();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        "SSL",
                        SslConfigs.SSL_PROTOCOL_CONFIG,
                        "TLSv1.3",
                        SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                        "TLSv1.2,TLSv1.3",
                        SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                        "JKS",
                        SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                        "");
        assertThat(props).doesNotContainKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(props).doesNotContainKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        assertThat(props).doesNotContainKey(SslConfigs.SSL_CIPHER_SUITES_CONFIG);

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.keystorePath(),
                        () -> config.keystorePassword(),
                        () -> config.keystoreType(),
                        () -> config.keyPassword());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Key store is not enabled. Check parameter [encryption.keystore.enable]");
        }
    }

    @Test
    public void shouldOverrideEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.put(EncryptionConfigs.SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        updatedConfig.put(EncryptionConfigs.SSL_PROTOCOL, "TLSv1.2");
        updatedConfig.put(
                EncryptionConfigs.SSL_CIPHER_SUITES,
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        updatedConfig.put(EncryptionConfigs.ENABLE_HOSTNAME_VERIFICATION, "true");
        updatedConfig.put(
                EncryptionConfigs.TRUSTSTORE_PATH, trustStoreFile.getFileName().toString());
        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_TYPE, "PKCS12");
        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PASSWORD, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [encryption.truststore.password]");
        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PASSWORD, "truststore-password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isEncryptionEnabled()).isTrue();
        assertThat(config.enabledProtocols()).containsExactly(TLSv12);
        assertThat(config.enabledProtocolsAsStr()).isEqualTo("TLSv1.2");
        assertThat(config.sslProtocol().toString()).isEqualTo("TLSv1.2");
        assertThat(config.truststoreType().toString()).isEqualTo("PKCS12");
        assertThat(config.truststorePath()).isEqualTo(trustStoreFile.toString());
        assertThat(config.truststorePassword()).isEqualTo("truststore-password");
        assertThat(config.isHostNameVerificationEnabled()).isTrue();
        assertThat(config.cipherSuites())
                .containsExactly(
                        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.cipherSuitesAsStr())
                .isEqualTo("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.sslProvider()).isNull();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .doesNotContainKey(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        assertThat(props)
                .containsAtLeast(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        "SSL",
                        SslConfigs.SSL_PROTOCOL_CONFIG,
                        "TLSv1.2",
                        SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                        "TLSv1.2",
                        SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                        "PKCS12",
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                        trustStoreFile.toString(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                        "truststore-password",
                        SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    @Test
    public void shouldSpecifyRequiredKeystoreParameters() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.put(EncryptionConfigs.ENABLE_MTLS, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [encryption.keystore.path]");

        updatedConfig.put(EncryptionConfigs.KEYSTORE_PATH, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [encryption.keystore.path]");

        updatedConfig.put(EncryptionConfigs.KEYSTORE_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Not found file ["
                                + adapterDir.toString()
                                + "/aFile] specified in [encryption.keystore.path]");

        updatedConfig.put(EncryptionConfigs.KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldGetDefaultKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.putAll(keystoreParameters());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isKeystoreEnabled()).isTrue();
        assertThat(config.keystorePath()).isEqualTo(keyStoreFile.toString());
        assertThat(config.keystoreType().toString()).isEqualTo("JKS");
        assertThat(config.keystorePassword()).isNull();
        assertThat(config.keyPassword()).isNull();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                        "JKS",
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        keyStoreFile.toString());
        assertThat(props).doesNotContainKey(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    }

    @Test
    public void shouldOverrideKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.putAll(keystoreParameters());
        updatedConfig.put(EncryptionConfigs.KEYSTORE_TYPE, "PKCS12");
        updatedConfig.put(EncryptionConfigs.KEYSTORE_PASSWORD, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [encryption.keystore.password]");

        updatedConfig.put(EncryptionConfigs.KEYSTORE_PASSWORD, "keystore-password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isKeystoreEnabled()).isTrue();
        assertThat(config.keystoreType().toString()).isEqualTo("PKCS12");

        updatedConfig.put(EncryptionConfigs.KEY_PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [encryption.keystore.key.password]");

        updatedConfig.put(EncryptionConfigs.KEY_PASSWORD, "key-password");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.keyPassword()).isEqualTo("key-password");

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                        "PKCS12",
                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        "keystore-password",
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        keyStoreFile.toString(),
                        SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                        "key-password");
    }

    @Test
    public void shouldSpecifyAuthenticationRequiredParametersWithDefaultPlain() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");

        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "invalid");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [authentication.mechanism]");
        // Restore default SASL/PLAIN mechanism
        updatedConfig.remove(BrokerAuthenticationConfigs.SASL_MECHANISM);

        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [authentication.username]");

        updatedConfig.put(USERNAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [authentication.username]");

        updatedConfig.put(USERNAME, "username");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [authentication.password]");

        updatedConfig.put(PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [authentication.password]");

        updatedConfig.put(PASSWORD, "password");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @ParameterizedTest
    @ValueSource(strings = {"SCRAM-SHA-256", "SCRAM-SHA-512"})
    public void shouldSpecifyAuthenticationRequiredParametersWithSCRAM(String saslMechanism) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, saslMechanism);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [authentication.username]");

        updatedConfig.put(USERNAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [authentication.username]");

        updatedConfig.put(USERNAME, "username");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [authentication.password]");

        updatedConfig.put(PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [authentication.password]");

        updatedConfig.put(PASSWORD, "password");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldNotAccessToAuthenticationSettings() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        assertThat(config.isAuthenticationEnabled()).isFalse();

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.authenticationMechanism(),
                        () -> config.authenticationUsername(),
                        () -> config.authenticationPassword(),
                        () -> config.isAwsMskIamEnabled(),
                        () -> config.awsMskIamCredentialProfileName(),
                        () -> config.awsMskIamRoleArn(),
                        () -> config.awsMskIamRoleSessionName(),
                        () -> config.awsMskIamStsRegion(),
                        () -> config.isGssapiEnabled(),
                        () -> config.gssapiKerberosServiceName(),
                        () -> config.gssapiKeyTab(),
                        () -> config.gssapiStoreKey(),
                        () -> config.gssapiPrincipal(),
                        () -> config.gssapiUseKeyTab(),
                        () -> config.gssapiUseTicketCache());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Authentication is not enabled. Check parameter [authentication.enable]");
        }
    }

    @Test
    public void shouldGetDefaultAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(authenticationParameters());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isAuthenticationEnabled()).isTrue();
        assertThat(config.authenticationMechanism().toString()).isEqualTo("PLAIN");

        Properties properties = config.baseConsumerProps();
        assertThat(properties)
                .containsAtLeast(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        "SASL_PLAINTEXT",
                        SaslConfigs.SASL_MECHANISM,
                        "PLAIN",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username='sasl-username' password='sasl-password';");
    }

    @Test
    public void shouldOverrideAuthenticationSettings() {
        // Sasl mechanisms under test
        List<SaslMechanism> mechanisms = List.of(SaslMechanism.SCRAM_256, SaslMechanism.SCRAM_512);

        for (boolean encrypted : List.of(true, false)) {
            Map<String, String> updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.putAll(authenticationParameters());
            // Test both encrypted and clear channels
            if (encrypted) {
                updatedConfig.putAll(encryptionParameters());
            }
            for (SaslMechanism mechanism : mechanisms) {
                updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, mechanism.toString());
                ConnectorConfig config =
                        ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

                assertThat(config.isAuthenticationEnabled()).isTrue();
                assertThat(config.authenticationMechanism().toString())
                        .isEqualTo(mechanism.toString());
                assertThat(config.authenticationUsername()).isEqualTo("sasl-username");
                assertThat(config.authenticationPassword()).isEqualTo("sasl-password");

                Properties properties = config.baseConsumerProps();
                assertThat(properties)
                        .containsAtLeast(
                                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                encrypted
                                        ? SecurityProtocol.SASL_SSL.toString()
                                        : SecurityProtocol.SASL_PLAINTEXT.toString(),
                                SaslConfigs.SASL_MECHANISM,
                                mechanism.toString(),
                                SaslConfigs.SASL_JAAS_CONFIG,
                                "org.apache.kafka.common.security.scram.ScramLoginModule required username='sasl-username' password='sasl-password';");
            }
        }
    }

    @Test
    public void shouldOverrideAuthenticationSettingsWithIam() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
        updatedConfig.put(ENCRYPTION_ENABLE, "true");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isAuthenticationEnabled()).isTrue();
        assertThat(config.authenticationMechanism()).isEqualTo(SaslMechanism.AWS_MSK_IAM);

        Properties properties = config.baseConsumerProps();
        assertThat(properties)
                .containsAtLeast(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        SecurityProtocol.SASL_SSL.toString(),
                        SaslConfigs.SASL_MECHANISM,
                        "AWS_MSK_IAM",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "software.amazon.msk.auth.iam.IAMLoginModule required;");

        updatedConfig.put(BrokerAuthenticationConfigs.AWS_MSK_IAM_CREDENTIAL_PROFILE_NAME, "");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [authentication.iam.credential.profile.name]");

        updatedConfig.put(
                BrokerAuthenticationConfigs.AWS_MSK_IAM_CREDENTIAL_PROFILE_NAME, "profileName");

        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isAuthenticationEnabled()).isTrue();
        assertThat(config.authenticationMechanism()).isEqualTo(SaslMechanism.AWS_MSK_IAM);
        properties = config.baseConsumerProps();
        assertThat(properties)
                .containsAtLeast(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        SecurityProtocol.SASL_SSL.toString(),
                        SaslConfigs.SASL_MECHANISM,
                        "AWS_MSK_IAM",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=\"profileName\";");

        updatedConfig.remove(BrokerAuthenticationConfigs.AWS_MSK_IAM_CREDENTIAL_PROFILE_NAME);

        updatedConfig.put(
                BrokerAuthenticationConfigs.AWS_MSK_IAM_ROLE_ARN,
                "arn:aws:iam::123456789012:role/roleName");
        updatedConfig.put(BrokerAuthenticationConfigs.AWS_MSK_IAM_ROLE_SESSION_NAME, "sessionName");
        updatedConfig.put(BrokerAuthenticationConfigs.AWS_MSK_IAM_STS_REGION, "us-west-2");

        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.baseConsumerProps())
                .containsAtLeast(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        SecurityProtocol.SASL_SSL.toString(),
                        SaslConfigs.SASL_MECHANISM,
                        "AWS_MSK_IAM",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\"arn:aws:iam::123456789012:role/roleName\" awsRoleSessionName=\"sessionName\" awsStsRegion=\"us-west-2\";");
    }

    @Test
    public void shouldSpecifyGssapiAuthenticationRequiredParameters() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [authentication.gssapi.kerberos.service.name]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [authentication.gssapi.kerberos.service.name]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [authentication.gssapi.principal]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [authentication.gssapi.principal]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldGetDefaultGssapiAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isGssapiEnabled()).isTrue();
        assertThat(config.gssapiUseKeyTab()).isFalse();
        assertThat(config.gssapiKeyTab()).isNull();
        assertThat(config.gssapiStoreKey()).isFalse();
        assertThat(config.gssapiPrincipal()).isEqualTo("kafka-user");
        assertThat(config.gssapiKerberosServiceName()).isEqualTo("kafka");
        assertThat(config.gssapiUseTicketCache()).isFalse();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        SaslConfigs.SASL_MECHANISM,
                        "GSSAPI",
                        SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
                        "kafka",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=false storeKey=false principal='kafka-user';");
    }

    @Test
    public void shouldOverrideGssapiAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_STORE_KEY_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE, "true");
        updatedConfig.put(
                BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH,
                keyTabFile.getFileName().toString());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isGssapiEnabled()).isTrue();
        assertThat(config.gssapiUseKeyTab()).isTrue();
        assertThat(config.gssapiKeyTab()).isEqualTo(keyTabFile.toString());
        assertThat(config.gssapiStoreKey()).isTrue();
        assertThat(config.gssapiPrincipal()).isEqualTo("kafka-user");
        assertThat(config.gssapiKerberosServiceName()).isEqualTo("kafka");

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        SaslConfigs.SASL_MECHANISM,
                        "GSSAPI",
                        SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
                        "kafka",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab='"
                                + keyTabFile.toAbsolutePath()
                                + "' principal='kafka-user';");
    }

    @Test
    public void shouldOverrideGssapiAuthenticationSettingsWithTicketCache() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_TICKET_CACHE_ENABLE, "true");

        // The following settings should be ignored.
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_STORE_KEY_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE, "true");
        updatedConfig.put(
                BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH,
                keyTabFile.getFileName().toString());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isGssapiEnabled()).isTrue();
        assertThat(config.gssapiUseTicketCache()).isTrue();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        SaslConfigs.SASL_MECHANISM,
                        "GSSAPI",
                        SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
                        "kafka",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true;");
    }

    @Test
    public void shouldNotValidateWhenKeyTabIsNotSpecified() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [authentication.gssapi.key.tab.path]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Not found file ["
                                + adapterDir.toString()
                                + "/aFile] specified in [authentication.gssapi.key.tab.path]");

        updatedConfig.put(
                BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH,
                keyTabFile.getFileName().toString());
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldNotValidateWhenPrincipalIsNotSpecifiedAndNotUseTicketCache() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [authentication.gssapi.key.tab.path]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Not found file ["
                                + adapterDir.toString()
                                + "/aFile] specified in [authentication.gssapi.key.tab.path]");

        updatedConfig.put(
                BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH,
                keyTabFile.getFileName().toString());
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldNotAccessToSchemaRegistrySettings() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        assertThat(config.isSchemaRegistryEnabled()).isFalse();
        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.isSchemaRegistryEncryptionEnabled(),
                        () -> config.schemaRegistryEnabledProtocols(),
                        () -> config.schemaRegistryEnabledProtocolsAsStr(),
                        () -> config.schemaRegistrySslProtocol(),
                        () -> config.schemaRegistryTruststoreType(),
                        () -> config.schemaRegistryTruststorePath(),
                        () -> config.schemaRegistryTruststorePassword(),
                        () -> config.isSchemaRegistryHostNameVerificationEnabled(),
                        () -> config.schemaRegistryCipherSuites(),
                        () -> config.schemaRegistryCipherSuitesAsStr(),
                        () -> config.schemaRegistrySslProvider(),
                        () -> config.isSchemaRegistryBasicAuthenticationEnabled());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Neither parameter [record.key.evaluator.schema.registry.enable] nor parameter [record.value.evaluator.schema.registry.enable] are enabled");
        }
    }

    @Test
    public void shouldNotAccessToSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://localhost:8080");
        assertThat(config.isSchemaRegistryEncryptionEnabled()).isFalse();

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.schemaRegistryEnabledProtocols(),
                        () -> config.schemaRegistryEnabledProtocolsAsStr(),
                        () -> config.schemaRegistrySslProtocol(),
                        () -> config.schemaRegistryTruststoreType(),
                        () -> config.schemaRegistryTruststorePath(),
                        () -> config.schemaRegistryTruststorePassword(),
                        () -> config.isSchemaRegistryHostNameVerificationEnabled(),
                        () -> config.schemaRegistryCipherSuites(),
                        () -> config.schemaRegistryCipherSuitesAsStr(),
                        () -> config.schemaRegistrySslProvider());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo("Parameter [schema.registry.url] is not set with https protocol");
        }
    }

    @Test
    public void shouldGetDefaultSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.schemaRegistryUrl()).isEqualTo("https://localhost:8080");

        assertThat(config.isSchemaRegistryEnabled()).isTrue();
        assertThat(config.schemaRegistryEnabledProtocols()).containsExactly(TLSv12, TLSv13);
        assertThat(config.schemaRegistryEnabledProtocolsAsStr()).isEqualTo("TLSv1.2,TLSv1.3");
        assertThat(config.schemaRegistrySslProtocol().toString()).isEqualTo("TLSv1.3");
        assertThat(config.schemaRegistryTruststoreType().toString()).isEqualTo("JKS");
        assertThat(config.schemaRegistryTruststorePath()).isNull();
        assertThat(config.schemaRegistryTruststorePassword()).isNull();
        assertThat(config.isSchemaRegistryHostNameVerificationEnabled()).isFalse();
        assertThat(config.schemaRegistryCipherSuites()).isEmpty();
        assertThat(config.schemaRegistryCipherSuitesAsStr()).isNull();
        assertThat(config.schemaRegistrySslProvider()).isNull();
        assertThat(config.isSchemaRegistryKeystoreEnabled()).isFalse();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        "schema.registry." + SslConfigs.SSL_PROTOCOL_CONFIG,
                        "TLSv1.3",
                        "schema.registry." + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                        "TLSv1.2,TLSv1.3",
                        "schema.registry." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                        "JKS",
                        "schema.registry."
                                + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                        "");
        assertThat(props)
                .doesNotContainKey("schema.registry." + SslConfigs.SSL_CIPHER_SUITES_CONFIG);

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.schemaRegistryKeystorePath(),
                        () -> config.schemaRegistryKeystorePassword(),
                        () -> config.schemaRegistryKeystoreType(),
                        () -> config.schemaRegistryKeyPassword());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Parameter [schema.registry.encryption.keystore.enable] is not enabled");
        }
    }

    @Test
    public void shouldOverrideSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");
        updatedConfig.put(TRUSTSTORE_PATH, trustStoreFile.getFileName().toString());
        updatedConfig.put(TRUSTSTORE_PASSWORD, "truststore-password");
        updatedConfig.put(SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        updatedConfig.put(SSL_PROTOCOL, "TLSv1.2");
        updatedConfig.put(TRUSTSTORE_TYPE, "PKCS12");
        updatedConfig.put(
                SSL_CIPHER_SUITES,
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        updatedConfig.put(HOSTNAME_VERIFICATION_ENABLE, "true");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isSchemaRegistryEncryptionEnabled()).isTrue();
        assertThat(config.schemaRegistryEnabledProtocols()).containsExactly(TLSv12);
        assertThat(config.schemaRegistryEnabledProtocolsAsStr()).isEqualTo("TLSv1.2");
        assertThat(config.schemaRegistrySslProtocol().toString()).isEqualTo("TLSv1.2");
        assertThat(config.schemaRegistryTruststoreType().toString()).isEqualTo("PKCS12");
        assertThat(config.schemaRegistryTruststorePath()).isEqualTo(trustStoreFile.toString());
        assertThat(config.schemaRegistryTruststorePassword()).isEqualTo("truststore-password");
        assertThat(config.isSchemaRegistryHostNameVerificationEnabled()).isTrue();
        assertThat(config.schemaRegistryCipherSuites())
                .containsExactly(
                        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.schemaRegistryCipherSuitesAsStr())
                .isEqualTo("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.schemaRegistrySslProvider()).isNull();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .doesNotContainKey(
                        "schema.registry."
                                + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        assertThat(props)
                .containsAtLeast(
                        "schema.registry." + SslConfigs.SSL_PROTOCOL_CONFIG,
                        "TLSv1.2",
                        "schema.registry." + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                        "TLSv1.2",
                        "schema.registry." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                        "PKCS12",
                        "schema.registry." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                        trustStoreFile.toString(),
                        "schema.registry." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                        "truststore-password",
                        "schema.registry." + SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
    }

    @Test
    public void shouldGetDefaultSchemaRegistryKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");
        updatedConfig.put(KEYSTORE_ENABLE, "true");
        updatedConfig.put(KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        updatedConfig.put(KEYSTORE_PASSWORD, "keystore-password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isSchemaRegistryKeystoreEnabled()).isTrue();
        assertThat(config.schemaRegistryKeystorePath()).isEqualTo(keyStoreFile.toString());
        assertThat(config.schemaRegistryKeystoreType().toString()).isEqualTo("JKS");
        assertThat(config.schemaRegistryKeystorePassword()).isEqualTo("keystore-password");
        assertThat(config.schemaRegistryKeyPassword()).isNull();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                        "JKS",
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        "keystore-password",
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        keyStoreFile.toString());
        assertThat(props)
                .doesNotContainKey("schema.registry." + SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    }

    @Test
    public void shouldOverrideSchemaRegistryKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");
        updatedConfig.put(KEYSTORE_ENABLE, "true");
        updatedConfig.put(KEYSTORE_TYPE, "PKCS12");
        updatedConfig.put(KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        updatedConfig.put(KEYSTORE_PASSWORD, "keystore-password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isSchemaRegistryKeystoreEnabled()).isTrue();
        assertThat(config.schemaRegistryKeystoreType().toString()).isEqualTo("PKCS12");

        updatedConfig.put(KEY_PASSWORD, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.encryption.keystore.key.password]");

        updatedConfig.put(KEY_PASSWORD, "key-password");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.schemaRegistryKeyPassword()).isEqualTo("key-password");

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                        "PKCS12",
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        "keystore-password",
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        keyStoreFile.toString(),
                        "schema.registry." + SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                        "key-password");
    }

    @Test
    public void shouldNotAccessToSchemaRegistryBasicAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryBasicAuthenticationEnabled()).isFalse();

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.schemaRegistryBasicAuthenticationUserName(),
                        () -> config.schemaRegistryBasicAuthenticationPassword());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Parameter [schema.registry.basic.authentication.enable] is not enabled");
        }
    }

    @Test
    public void shouldGetSchemaRegistryBasicAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");
        updatedConfig.put(ENABLE_BASIC_AUTHENTICATION, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [schema.registry.basic.authentication.username]");

        updatedConfig.put(BASIC_AUTHENTICATION_USER_NAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.basic.authentication.username]");

        updatedConfig.put(BASIC_AUTHENTICATION_USER_NAME, "username");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [schema.registry.basic.authentication.password]");

        updatedConfig.put(BASIC_AUTHENTICATION_USER_PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.basic.authentication.password]");

        updatedConfig.put(BASIC_AUTHENTICATION_USER_PASSWORD, "password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryBasicAuthenticationEnabled()).isTrue();
        assertThat(config.schemaRegistryBasicAuthenticationUserName()).isEqualTo("username");
        assertThat(config.schemaRegistryBasicAuthenticationPassword()).isEqualTo("password");

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        BASIC_AUTH_CREDENTIALS_SOURCE,
                        "USER_INFO",
                        USER_INFO_CONFIG,
                        "username:password");
    }
}
