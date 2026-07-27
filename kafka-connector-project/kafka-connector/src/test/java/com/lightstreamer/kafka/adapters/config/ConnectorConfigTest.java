
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
import static com.lightstreamer.kafka.adapters.config.BrokerAuthenticationConfigs.PASSWORD;
import static com.lightstreamer.kafka.adapters.config.BrokerAuthenticationConfigs.USERNAME;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.AUTHENTICATION_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.BOOTSTRAP_SERVERS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_CLIENT_ID;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_FETCH_MAX_BYTES_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_FETCH_MIN_BYTES_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_HEARTBEAT_INTERVAL_MS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_METADATA_MAX_AGE_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_MODE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.DATA_ADAPTER_NAME;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ENCRYPTION_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.FIELDS_MAP_NON_SCALAR_VALUES_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.FIELDS_SKIP_FAILED_MAPPING_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.FIELD_MAPPING;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.GROUP_ID;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ITEM_SNAPSHOT_DISTINCT_LENGTH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ITEM_SNAPSHOT_ENABLED_MODE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ITEM_SNAPSHOT_MAX_IDLE_SECONDS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.ITEM_TEMPLATE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.LIGHTSTREAMER_CLIENT_ID;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.MAP_FROM_PARTITIONS_SUFFIX;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.MAP_REG_EX_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.MAP_TO_ITEMS_SUFFIX;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_FROM;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_MAX_POLL_RECORDS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_NUM_THREADS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_ORDER_STRATEGY;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS;
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
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_BASIC_AUTHENTICATION_USER_NAME;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_BASIC_AUTHENTICATION_USER_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_HOSTNAME_VERIFICATION_ENABLE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_KEYSTORE_ENABLE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_KEYSTORE_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_KEYSTORE_PATH;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_KEYSTORE_TYPE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_KEY_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_SSL_CIPHER_SUITES;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_SSL_ENABLED_PROTOCOLS;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_SSL_PROTOCOL;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_TRUSTSTORE_PASSWORD;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_TRUSTSTORE_PATH;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.CONFLUENT_TRUSTSTORE_TYPE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.URL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.STRING;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom.EARLIEST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol.TLSv12;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol.TLSv13;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.SNAPSHOT_ENABLED_MODE;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.WrappedNoWildcardCheck;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.interfaces.metadata.Mode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.ConsumerMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.ItemSnapshotEnabledMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SchemaRegistryProvider;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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
import java.util.Set;
import java.util.stream.Stream;

class ConnectorConfigTest {

    private Path adapterDir;

    private Path avroKeySchemaFile;
    private Path avroValueSchemaFile;

    private Path protoKeySchemaFile;
    private Path protoValueSchemaFile;

    private Path trustStoreFile;
    private Path keyStoreFile;

    private Path keyTabFile;

    @BeforeEach
    void before() throws IOException {
        adapterDir = Files.createTempDirectory("myadapter_dir");
        avroKeySchemaFile = Files.createTempFile(adapterDir, "key-schema-", ".avsc");
        avroValueSchemaFile = Files.createTempFile(adapterDir, "value-schema-", ".avsc");
        protoKeySchemaFile = Files.createTempFile(adapterDir, "proto-key-schema-", ".desc");
        protoValueSchemaFile = Files.createTempFile(adapterDir, "proto-value-schema-", ".desc");
        trustStoreFile = Files.createTempFile(adapterDir, "truststore", ".jks");
        keyStoreFile = Files.createTempFile(adapterDir, "keystore", ".jks");
        keyTabFile = Files.createTempFile(adapterDir, "keytabFile", ".keytab");
    }

    @AfterEach
    void after() throws IOException {
        Files.delete(avroKeySchemaFile);
        Files.delete(avroValueSchemaFile);
        Files.delete(protoKeySchemaFile);
        Files.delete(protoValueSchemaFile);
        Files.delete(trustStoreFile);
        Files.delete(keyStoreFile);
        Files.delete(keyTabFile);
        FileUtils.deleteDirectory(adapterDir.toFile());
    }

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = ConnectorConfig.configSpec();

        ConfParameter adapterConfId = configSpec.findParameter(ADAPTERS_CONF_ID);
        assertThat(adapterConfId.name()).isEqualTo(ADAPTERS_CONF_ID);
        assertThat(adapterConfId.required()).isTrue();
        assertThat(adapterConfId.multiple()).isFalse();
        assertThat(adapterConfId.mutable()).isTrue();
        assertThat(adapterConfId.defaultValue()).isNull();
        assertThat(adapterConfId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter dataAdapterName = configSpec.findParameter(DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.name()).isEqualTo(DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.required()).isTrue();
        assertThat(dataAdapterName.multiple()).isFalse();
        assertThat(dataAdapterName.mutable()).isTrue();
        assertThat(dataAdapterName.defaultValue()).isNull();
        assertThat(dataAdapterName.type()).isEqualTo(ConfType.TEXT);

        ConfParameter enabled = configSpec.findParameter(ENABLE);
        assertThat(enabled.name()).isEqualTo(ENABLE);
        assertThat(enabled.required()).isFalse();
        assertThat(enabled.multiple()).isFalse();
        assertThat(enabled.mutable()).isTrue();
        assertThat(enabled.defaultValue()).isEqualTo("true");
        assertThat(enabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter bootStrapServers = configSpec.findParameter(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.name()).isEqualTo(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.required()).isTrue();
        assertThat(bootStrapServers.multiple()).isFalse();
        assertThat(bootStrapServers.mutable()).isTrue();
        assertThat(bootStrapServers.defaultValue()).isNull();
        assertThat(bootStrapServers.type()).isEqualTo(ConfType.HOST_LIST);

        ConfParameter groupId = configSpec.findParameter(GROUP_ID);
        assertThat(groupId.name()).isEqualTo(GROUP_ID);
        assertThat(groupId.required()).isFalse();
        assertThat(groupId.multiple()).isFalse();
        assertThat(groupId.mutable()).isTrue();
        assertThat(groupId.defaultValue()).isNotNull();
        assertThat(groupId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemTemplate = configSpec.findParameter(ITEM_TEMPLATE);
        assertThat(itemTemplate.name()).isEqualTo(ITEM_TEMPLATE);
        assertThat(itemTemplate.required()).isFalse();
        assertThat(itemTemplate.multiple()).isTrue();
        assertThat(itemTemplate.suffix()).isNull();
        assertThat(itemTemplate.mutable()).isTrue();
        assertThat(itemTemplate.defaultValue()).isNull();
        assertThat(itemTemplate.type()).isEqualTo(ConfType.TEXT);

        ConfParameter topicMapping = configSpec.findParameter(TOPIC_MAPPING, MAP_TO_ITEMS_SUFFIX);
        assertThat(topicMapping.name()).isEqualTo(TOPIC_MAPPING);
        assertThat(topicMapping.required()).isTrue();
        assertThat(topicMapping.multiple()).isTrue();
        assertThat(topicMapping.suffix()).isEqualTo("to");
        assertThat(topicMapping.mutable()).isTrue();
        assertThat(topicMapping.defaultValue()).isNull();
        assertThat(topicMapping.type()).isEqualTo(ConfType.TEXT_LIST);

        ConfParameter partitionMapping =
                configSpec.findParameter(TOPIC_MAPPING, MAP_FROM_PARTITIONS_SUFFIX);
        assertThat(partitionMapping.name()).isEqualTo(TOPIC_MAPPING);
        assertThat(partitionMapping.required()).isFalse();
        assertThat(partitionMapping.multiple()).isTrue();
        assertThat(partitionMapping.suffix()).isEqualTo("from.partitions");
        assertThat(partitionMapping.mutable()).isTrue();
        assertThat(partitionMapping.defaultValue()).isNull();
        assertThat(partitionMapping.type()).isEqualTo(ConfType.TEXT_LIST);

        ConfParameter mapRegExEnable = configSpec.findParameter(MAP_REG_EX_ENABLE);
        assertThat(mapRegExEnable.name()).isEqualTo(MAP_REG_EX_ENABLE);
        assertThat(mapRegExEnable.required()).isFalse();
        assertThat(mapRegExEnable.multiple()).isFalse();
        assertThat(mapRegExEnable.suffix()).isNull();
        assertThat(mapRegExEnable.mutable()).isTrue();
        assertThat(mapRegExEnable.defaultValue()).isEqualTo("false");
        assertThat(mapRegExEnable.type()).isEqualTo(ConfType.BOOL);

        ConfParameter fieldMapping = configSpec.findParameter(FIELD_MAPPING);
        assertThat(fieldMapping.name()).isEqualTo(FIELD_MAPPING);
        assertThat(fieldMapping.required()).isTrue();
        assertThat(fieldMapping.multiple()).isTrue();
        assertThat(fieldMapping.suffix()).isNull();
        assertThat(fieldMapping.mutable()).isTrue();
        assertThat(fieldMapping.defaultValue()).isNull();
        assertThat(fieldMapping.type()).isEqualTo(ConfType.TEXT);

        ConfParameter fieldsSkipFailedMappingEnable =
                configSpec.findParameter(FIELDS_SKIP_FAILED_MAPPING_ENABLE);
        assertThat(fieldsSkipFailedMappingEnable.name())
                .isEqualTo(FIELDS_SKIP_FAILED_MAPPING_ENABLE);
        assertThat(fieldsSkipFailedMappingEnable.required()).isFalse();
        assertThat(fieldsSkipFailedMappingEnable.multiple()).isFalse();
        assertThat(fieldsSkipFailedMappingEnable.suffix()).isNull();
        assertThat(fieldsSkipFailedMappingEnable.mutable()).isTrue();
        assertThat(fieldsSkipFailedMappingEnable.defaultValue()).isEqualTo("false");
        assertThat(fieldsSkipFailedMappingEnable.type()).isEqualTo(ConfType.BOOL);

        ConfParameter fieldsMapNonScalarValuesEnable =
                configSpec.findParameter(FIELDS_MAP_NON_SCALAR_VALUES_ENABLE);
        assertThat(fieldsMapNonScalarValuesEnable.name())
                .isEqualTo(FIELDS_MAP_NON_SCALAR_VALUES_ENABLE);
        assertThat(fieldsMapNonScalarValuesEnable.required()).isFalse();
        assertThat(fieldsMapNonScalarValuesEnable.multiple()).isFalse();
        assertThat(fieldsMapNonScalarValuesEnable.suffix()).isNull();
        assertThat(fieldsMapNonScalarValuesEnable.mutable()).isTrue();
        assertThat(fieldsMapNonScalarValuesEnable.defaultValue()).isEqualTo("false");
        assertThat(fieldsMapNonScalarValuesEnable.type()).isEqualTo(ConfType.BOOL);

        ConfParameter keyEvaluatorType = configSpec.findParameter(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.name()).isEqualTo(RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.required()).isFalse();
        assertThat(keyEvaluatorType.multiple()).isFalse();
        assertThat(keyEvaluatorType.mutable()).isTrue();
        assertThat(keyEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(keyEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter keySchemaPath = configSpec.findParameter(RECORD_KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaPath.name()).isEqualTo(RECORD_KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaPath.required()).isFalse();
        assertThat(keySchemaPath.multiple()).isFalse();
        assertThat(keySchemaPath.mutable()).isTrue();
        assertThat(keySchemaPath.defaultValue()).isNull();
        assertThat(keySchemaPath.type()).isEqualTo(ConfType.FILE);

        ConfParameter schemaRegistryEnabledForKey =
                configSpec.findParameter(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForKey.name())
                .isEqualTo(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForKey.required()).isFalse();
        assertThat(schemaRegistryEnabledForKey.multiple()).isFalse();
        assertThat(schemaRegistryEnabledForKey.mutable()).isTrue();
        assertThat(schemaRegistryEnabledForKey.defaultValue()).isEqualTo("false");
        assertThat(schemaRegistryEnabledForKey.type()).isEqualTo(ConfType.BOOL);

        ConfParameter valueEvaluatorType = configSpec.findParameter(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.name()).isEqualTo(RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.required()).isFalse();
        assertThat(valueEvaluatorType.multiple()).isFalse();
        assertThat(valueEvaluatorType.mutable()).isTrue();
        assertThat(valueEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(valueEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter valueSchemaPath =
                configSpec.findParameter(RECORD_VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaPath.name()).isEqualTo(RECORD_VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaPath.required()).isFalse();
        assertThat(valueSchemaPath.multiple()).isFalse();
        assertThat(valueSchemaPath.mutable()).isTrue();
        assertThat(valueSchemaPath.defaultValue()).isNull();
        assertThat(valueSchemaPath.type()).isEqualTo(ConfType.FILE);

        ConfParameter schemaRegistryEnabledForValue =
                configSpec.findParameter(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForValue.name())
                .isEqualTo(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForValue.required()).isFalse();
        assertThat(schemaRegistryEnabledForValue.multiple()).isFalse();
        assertThat(schemaRegistryEnabledForValue.mutable()).isTrue();
        assertThat(schemaRegistryEnabledForValue.defaultValue()).isEqualTo("false");
        assertThat(schemaRegistryEnabledForValue.type()).isEqualTo(ConfType.BOOL);

        ConfParameter keyKvpSeparator =
                configSpec.findParameter(RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(keyKvpSeparator.name()).isEqualTo(RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(keyKvpSeparator.required()).isFalse();
        assertThat(keyKvpSeparator.multiple()).isFalse();
        assertThat(keyKvpSeparator.mutable()).isTrue();
        assertThat(keyKvpSeparator.defaultValue()).isEqualTo(",");
        assertThat(keyKvpSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter keyKvpKeyValueSeparator =
                configSpec.findParameter(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(keyKvpKeyValueSeparator.name())
                .isEqualTo(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(keyKvpKeyValueSeparator.required()).isFalse();
        assertThat(keyKvpKeyValueSeparator.multiple()).isFalse();
        assertThat(keyKvpKeyValueSeparator.mutable()).isTrue();
        assertThat(keyKvpKeyValueSeparator.defaultValue()).isEqualTo("=");
        assertThat(keyKvpKeyValueSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter valueKvpSeparator =
                configSpec.findParameter(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(valueKvpSeparator.name()).isEqualTo(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR);
        assertThat(valueKvpSeparator.required()).isFalse();
        assertThat(valueKvpSeparator.multiple()).isFalse();
        assertThat(valueKvpSeparator.mutable()).isTrue();
        assertThat(valueKvpSeparator.defaultValue()).isEqualTo(",");
        assertThat(valueKvpSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter valueKvpKeyValueSeparator =
                configSpec.findParameter(RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(valueKvpKeyValueSeparator.name())
                .isEqualTo(RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR);
        assertThat(valueKvpKeyValueSeparator.required()).isFalse();
        assertThat(valueKvpKeyValueSeparator.multiple()).isFalse();
        assertThat(valueKvpKeyValueSeparator.mutable()).isTrue();
        assertThat(valueKvpKeyValueSeparator.defaultValue()).isEqualTo("=");
        assertThat(valueKvpKeyValueSeparator.type()).isEqualTo(ConfType.CHAR);

        ConfParameter keyEvaluatorProtobufMessageType =
                configSpec.findParameter(RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(keyEvaluatorProtobufMessageType.name())
                .isEqualTo(RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(keyEvaluatorProtobufMessageType.required()).isFalse();
        assertThat(keyEvaluatorProtobufMessageType.multiple()).isFalse();
        assertThat(keyEvaluatorProtobufMessageType.mutable()).isTrue();
        assertThat(keyEvaluatorProtobufMessageType.defaultValue()).isNull();
        assertThat(keyEvaluatorProtobufMessageType.type()).isEqualTo(ConfType.TEXT);

        ConfParameter valueEvaluatorProtobufMessageType =
                configSpec.findParameter(RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(valueEvaluatorProtobufMessageType.name())
                .isEqualTo(RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
        assertThat(valueEvaluatorProtobufMessageType.required()).isFalse();
        assertThat(valueEvaluatorProtobufMessageType.multiple()).isFalse();
        assertThat(valueEvaluatorProtobufMessageType.mutable()).isTrue();
        assertThat(valueEvaluatorProtobufMessageType.defaultValue()).isNull();
        assertThat(valueEvaluatorProtobufMessageType.type()).isEqualTo(ConfType.TEXT);

        ConfParameter errorHandlingStrategy =
                configSpec.findParameter(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY);
        assertThat(errorHandlingStrategy.name())
                .isEqualTo(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY);
        assertThat(errorHandlingStrategy.required()).isFalse();
        assertThat(errorHandlingStrategy.multiple()).isFalse();
        assertThat(errorHandlingStrategy.mutable()).isTrue();
        assertThat(errorHandlingStrategy.defaultValue()).isEqualTo("IGNORE_AND_CONTINUE");
        assertThat(errorHandlingStrategy.type()).isEqualTo(ConfType.ERROR_STRATEGY);

        ConfParameter recordConsumeWithOrderStrategy =
                configSpec.findParameter(RECORD_CONSUME_WITH_ORDER_STRATEGY);
        assertThat(recordConsumeWithOrderStrategy.name())
                .isEqualTo(RECORD_CONSUME_WITH_ORDER_STRATEGY);
        assertThat(recordConsumeWithOrderStrategy.required()).isFalse();
        assertThat(recordConsumeWithOrderStrategy.multiple()).isFalse();
        assertThat(recordConsumeWithOrderStrategy.mutable()).isTrue();
        assertThat(recordConsumeWithOrderStrategy.defaultValue()).isEqualTo("ORDER_BY_PARTITION");
        assertThat(recordConsumeWithOrderStrategy.type()).isEqualTo(ConfType.ORDER_STRATEGY);

        ConfParameter recordConsumeWithThreadsNumber =
                configSpec.findParameter(RECORD_CONSUME_WITH_NUM_THREADS);
        assertThat(recordConsumeWithThreadsNumber.name())
                .isEqualTo(RECORD_CONSUME_WITH_NUM_THREADS);
        assertThat(recordConsumeWithThreadsNumber.required()).isFalse();
        assertThat(recordConsumeWithThreadsNumber.multiple()).isFalse();
        assertThat(recordConsumeWithThreadsNumber.mutable()).isTrue();
        assertThat(recordConsumeWithThreadsNumber.defaultValue()).isEqualTo("1");
        assertThat(recordConsumeWithThreadsNumber.type()).isEqualTo(ConfType.THREADS);

        ConfParameter enableAutoCommit =
                configSpec.findParameter(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.name()).isEqualTo(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.required()).isTrue();
        assertThat(enableAutoCommit.multiple()).isFalse();
        assertThat(enableAutoCommit.mutable()).isFalse();
        assertThat(enableAutoCommit.defaultValue()).isEqualTo("false");
        assertThat(enableAutoCommit.type()).isEqualTo(ConfType.BOOL);

        ConfParameter encryptionEnabled = configSpec.findParameter(ENCRYPTION_ENABLE);
        assertThat(encryptionEnabled.name()).isEqualTo(ENCRYPTION_ENABLE);
        assertThat(encryptionEnabled.required()).isFalse();
        assertThat(encryptionEnabled.multiple()).isFalse();
        assertThat(encryptionEnabled.mutable()).isTrue();
        assertThat(encryptionEnabled.defaultValue()).isEqualTo("false");
        assertThat(encryptionEnabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter authenticationEnabled = configSpec.findParameter(AUTHENTICATION_ENABLE);
        assertThat(authenticationEnabled.name()).isEqualTo(AUTHENTICATION_ENABLE);
        assertThat(authenticationEnabled.required()).isFalse();
        assertThat(authenticationEnabled.multiple()).isFalse();
        assertThat(authenticationEnabled.mutable()).isTrue();
        assertThat(authenticationEnabled.defaultValue()).isEqualTo("false");
        assertThat(authenticationEnabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter consumeEventsFrom = configSpec.findParameter(RECORD_CONSUME_FROM);
        assertThat(consumeEventsFrom.name()).isEqualTo(RECORD_CONSUME_FROM);
        assertThat(consumeEventsFrom.required()).isFalse();
        assertThat(consumeEventsFrom.multiple()).isFalse();
        assertThat(consumeEventsFrom.mutable()).isTrue();
        assertThat(consumeEventsFrom.defaultValue()).isEqualTo("LATEST");
        assertThat(consumeEventsFrom.type()).isEqualTo(ConfType.CONSUME_FROM);

        ConfParameter clientId = configSpec.findParameter(CONSUMER_CLIENT_ID);
        assertThat(clientId.name()).isEqualTo(CONSUMER_CLIENT_ID);
        assertThat(clientId.required()).isTrue();
        assertThat(clientId.required()).isTrue();
        assertThat(clientId.multiple()).isFalse();
        assertThat(clientId.mutable()).isFalse();
        assertThat(clientId.defaultValue()).isEqualTo("");
        assertThat(clientId.type()).isEqualTo(ConfType.BLANKABLE_TEXT);

        ConfParameter enableAutoCommitConfig =
                configSpec.findParameter(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommitConfig.name()).isEqualTo(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommitConfig.required()).isTrue();
        assertThat(enableAutoCommitConfig.multiple()).isFalse();
        assertThat(enableAutoCommitConfig.mutable()).isFalse();
        assertThat(enableAutoCommitConfig.defaultValue()).isEqualTo("false");
        assertThat(enableAutoCommitConfig.type()).isEqualTo(ConfType.BOOL);

        ConfParameter reconnectBackoffMaxMs =
                configSpec.findParameter(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        assertThat(reconnectBackoffMaxMs.name())
                .isEqualTo(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        assertThat(reconnectBackoffMaxMs.required()).isFalse();
        assertThat(reconnectBackoffMaxMs.multiple()).isFalse();
        assertThat(reconnectBackoffMaxMs.mutable()).isTrue();
        assertThat(reconnectBackoffMaxMs.defaultValue()).isNull();
        assertThat(reconnectBackoffMaxMs.type()).isEqualTo(ConfType.NON_NEGATIVE_INT);

        ConfParameter reconnectBackoffMs =
                configSpec.findParameter(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG);
        assertThat(reconnectBackoffMs.name()).isEqualTo(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG);
        assertThat(reconnectBackoffMs.required()).isFalse();
        assertThat(reconnectBackoffMs.multiple()).isFalse();
        assertThat(reconnectBackoffMs.mutable()).isTrue();
        assertThat(reconnectBackoffMs.defaultValue()).isNull();
        assertThat(reconnectBackoffMs.type()).isEqualTo(ConfType.NON_NEGATIVE_INT);

        ConfParameter fetchMinBytes = configSpec.findParameter(CONSUMER_FETCH_MIN_BYTES_CONFIG);
        assertThat(fetchMinBytes.name()).isEqualTo(CONSUMER_FETCH_MIN_BYTES_CONFIG);
        assertThat(fetchMinBytes.required()).isFalse();
        assertThat(fetchMinBytes.multiple()).isFalse();
        assertThat(fetchMinBytes.mutable()).isTrue();
        assertThat(fetchMinBytes.defaultValue()).isNull();
        assertThat(fetchMinBytes.type()).isEqualTo(ConfType.NON_NEGATIVE_INT);

        ConfParameter fetchMaxBytes = configSpec.findParameter(CONSUMER_FETCH_MAX_BYTES_CONFIG);
        assertThat(fetchMaxBytes.name()).isEqualTo(CONSUMER_FETCH_MAX_BYTES_CONFIG);
        assertThat(fetchMaxBytes.required()).isFalse();
        assertThat(fetchMaxBytes.multiple()).isFalse();
        assertThat(fetchMaxBytes.mutable()).isTrue();
        assertThat(fetchMaxBytes.defaultValue()).isNull();
        assertThat(fetchMaxBytes.type()).isEqualTo(ConfType.NON_NEGATIVE_INT);

        ConfParameter fetchMaxWaitMs = configSpec.findParameter(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG);
        assertThat(fetchMaxWaitMs.name()).isEqualTo(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG);
        assertThat(fetchMaxWaitMs.required()).isFalse();
        assertThat(fetchMaxWaitMs.multiple()).isFalse();
        assertThat(fetchMaxWaitMs.mutable()).isTrue();
        assertThat(fetchMaxWaitMs.defaultValue()).isNull();
        assertThat(fetchMaxWaitMs.type()).isEqualTo(ConfType.NON_NEGATIVE_INT);

        ConfParameter maxPollRecords =
                configSpec.findParameter(RECORD_CONSUME_WITH_MAX_POLL_RECORDS);
        assertThat(maxPollRecords.name()).isEqualTo(RECORD_CONSUME_WITH_MAX_POLL_RECORDS);
        assertThat(maxPollRecords.required()).isFalse();
        assertThat(maxPollRecords.multiple()).isFalse();
        assertThat(maxPollRecords.mutable()).isTrue();
        assertThat(maxPollRecords.defaultValue()).isEqualTo("500");
        assertThat(maxPollRecords.type()).isEqualTo(ConfType.POSITIVE_INT);

        ConfParameter heartBeatIntervalMs =
                configSpec.findParameter(CONSUMER_HEARTBEAT_INTERVAL_MS);
        assertThat(heartBeatIntervalMs.name()).isEqualTo(CONSUMER_HEARTBEAT_INTERVAL_MS);
        assertThat(heartBeatIntervalMs.required()).isFalse();
        assertThat(heartBeatIntervalMs.multiple()).isFalse();
        assertThat(heartBeatIntervalMs.mutable()).isTrue();
        assertThat(heartBeatIntervalMs.defaultValue()).isNull();
        assertThat(heartBeatIntervalMs.type()).isEqualTo(ConfType.INT);

        ConfParameter sessionTimeoutMs =
                configSpec.findParameter(RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS);
        assertThat(sessionTimeoutMs.name()).isEqualTo(RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS);
        assertThat(sessionTimeoutMs.required()).isFalse();
        assertThat(sessionTimeoutMs.multiple()).isFalse();
        assertThat(sessionTimeoutMs.mutable()).isTrue();
        assertThat(sessionTimeoutMs.defaultValue()).isEqualTo("45000");
        assertThat(sessionTimeoutMs.type()).isEqualTo(ConfType.INT);

        ConfParameter maxPollIntervalMs =
                configSpec.findParameter(RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS);
        assertThat(maxPollIntervalMs.name()).isEqualTo(RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS);
        assertThat(maxPollIntervalMs.required()).isFalse();
        assertThat(maxPollIntervalMs.multiple()).isFalse();
        assertThat(maxPollIntervalMs.mutable()).isTrue();
        assertThat(maxPollIntervalMs.defaultValue()).isEqualTo("30000");
        assertThat(maxPollIntervalMs.type()).isEqualTo(ConfType.POSITIVE_INT);

        ConfParameter metadataMaxAge = configSpec.findParameter(CONSUMER_METADATA_MAX_AGE_CONFIG);
        assertThat(metadataMaxAge.name()).isEqualTo(CONSUMER_METADATA_MAX_AGE_CONFIG);
        assertThat(metadataMaxAge.required()).isTrue();
        assertThat(metadataMaxAge.multiple()).isFalse();
        assertThat(metadataMaxAge.mutable()).isFalse();
        assertThat(metadataMaxAge.defaultValue()).isEqualTo("250");
        assertThat(metadataMaxAge.type()).isEqualTo(ConfType.INT);

        ConfParameter requestTimeoutMs =
                configSpec.findParameter(CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        assertThat(requestTimeoutMs.name()).isEqualTo(CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        assertThat(requestTimeoutMs.required()).isTrue();
        assertThat(requestTimeoutMs.multiple()).isFalse();
        assertThat(requestTimeoutMs.mutable()).isFalse();
        assertThat(requestTimeoutMs.defaultValue()).isEqualTo("30000");
        assertThat(requestTimeoutMs.type()).isEqualTo(ConfType.INT);

        ConfParameter itemSnapShotEnableMode = configSpec.findParameter(ITEM_SNAPSHOT_ENABLED_MODE);
        assertThat(itemSnapShotEnableMode.name()).isEqualTo(ITEM_SNAPSHOT_ENABLED_MODE);
        assertThat(itemSnapShotEnableMode.required()).isFalse();
        assertThat(itemSnapShotEnableMode.multiple()).isFalse();
        assertThat(itemSnapShotEnableMode.mutable()).isTrue();
        assertThat(itemSnapShotEnableMode.defaultValue()).isEqualTo("NONE");
        assertThat(itemSnapShotEnableMode.type()).isEqualTo(SNAPSHOT_ENABLED_MODE);

        ConfParameter itemSnapShotDistinctLength =
                configSpec.findParameter(ITEM_SNAPSHOT_DISTINCT_LENGTH);
        assertThat(itemSnapShotDistinctLength.name()).isEqualTo(ITEM_SNAPSHOT_DISTINCT_LENGTH);
        assertThat(itemSnapShotDistinctLength.required()).isFalse();
        assertThat(itemSnapShotDistinctLength.multiple()).isFalse();
        assertThat(itemSnapShotDistinctLength.mutable()).isTrue();
        assertThat(itemSnapShotDistinctLength.defaultValue()).isEqualTo("10");
        assertThat(itemSnapShotDistinctLength.type()).isEqualTo(ConfType.POSITIVE_INT);

        ConfParameter itemSnapShotMaxIdleSeconds =
                configSpec.findParameter(ITEM_SNAPSHOT_MAX_IDLE_SECONDS);
        assertThat(itemSnapShotMaxIdleSeconds.name()).isEqualTo(ITEM_SNAPSHOT_MAX_IDLE_SECONDS);
        assertThat(itemSnapShotMaxIdleSeconds.required()).isFalse();
        assertThat(itemSnapShotMaxIdleSeconds.multiple()).isFalse();
        assertThat(itemSnapShotMaxIdleSeconds.mutable()).isTrue();
        assertThat(itemSnapShotMaxIdleSeconds.defaultValue()).isEqualTo("0");
        assertThat(itemSnapShotMaxIdleSeconds.type()).isEqualTo(ConfType.NON_NEGATIVE_INT);
    }

    private Map<String, String> standardParameters() {
        Map<String, String> standardParams = new HashMap<>();
        standardParams.put(BOOTSTRAP_SERVERS, "server:8080,server:8081");
        standardParams.put(RECORD_VALUE_EVALUATOR_TYPE, "STRING");
        // standardParams.put(RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
        // valueScheFile.getFileName().toString());
        standardParams.put(RECORD_KEY_EVALUATOR_TYPE, "JSON");
        // standardParams.put(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH,keySchemaFile.getFileName().toString());
        standardParams.put(ADAPTERS_CONF_ID, "KAFKA");
        standardParams.put(DATA_ADAPTER_NAME, "CONNECTOR");
        // standardParams.put(ConnectorConfig.CONSUMER_CLIENT_ID, "a.client.id"); // Unmodifiable
        standardParams.put(CONSUMER_FETCH_MAX_BYTES_CONFIG, "100");
        standardParams.put(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG, "200");
        standardParams.put(CONSUMER_FETCH_MIN_BYTES_CONFIG, "300");
        standardParams.put(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG, "400");
        standardParams.put(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG, "500");
        standardParams.put(RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS, "5000");
        standardParams.put(RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS, "800");
        standardParams.put(CONSUMER_HEARTBEAT_INTERVAL_MS, "600");
        standardParams.put(RECORD_CONSUME_WITH_MAX_POLL_RECORDS, "700");
        standardParams.put(CONSUMER_METADATA_MAX_AGE_CONFIG, "250"); // Unmodifiable
        standardParams.put(CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG, "1000"); // Unmodifiable
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
    void shouldSpecifyRequiredParams() {
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
    void shouldRetrieveConfiguration() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        Map<String, String> configuration = config.configuration();
        assertThat(configuration).isNotEmpty();
    }

    @Test
    void shouldRetrieveBaseConsumerProperties() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        Properties baseConsumerProps = config.baseConsumerProps();
        assertThat(baseConsumerProps)
                .containsAtLeast(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArrayDeserializer",
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
    void shouldRetrieveLightstreamerClientIdWhenConnectedToConfluentClod(String hostList) {
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
    void shouldNonRetrieveLightstreamerClientIdWhenNotAllHostConnectedToConfluentClod(
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
    void shouldExtendBaseConsumerProperties() {
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
    void shouldNotModifyEnableAutoCommitConfig() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG, "true");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getBoolean(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG)).isFalse();
    }

    @Test
    void shouldGetText() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.getMetadataAdapterName()).isEqualTo("KAFKA");
        assertThat(config.getAdapterName()).isEqualTo("CONNECTOR");

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
            updatedConfig.put(
                    SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                    SchemaRegistryConfigs.DEFAULT_SCHEMA_REGISTRY_PROVIDER.toString());
        }
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getValueEvaluator()).isEqualTo(EvaluatorType.valueOf(type));
        assertThat(config.getKeyEvaluator()).isEqualTo(EvaluatorType.valueOf(type));
    }

    @Test
    void shouldFailDueToInvalidEvaluatorType() {
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
    void shouldFailDueToInvalidSchemaPath() {
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
    void shouldSpecifyRequiredParamsForAvro() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
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
                                        "http://localhost:8081",
                                        SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                        SchemaRegistryConfigs.DEFAULT_SCHEMA_REGISTRY_PROVIDER
                                                .toString())));

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
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
    void shouldSpecifyRequiredParamsForProtobuf() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
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
                                        adapterDir.toString(),
                                        Map.of(
                                                RECORD_KEY_EVALUATOR_TYPE,
                                                "PROTOBUF",
                                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                                "true",
                                                SchemaRegistryConfigs.URL,
                                                "http://localhost:8081",
                                                SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                                SchemaRegistryProvider.AZURE.toString(),
                                                SchemaRegistryConfigs.AZURE_CLIENT_ID,
                                                "azure-client-id",
                                                SchemaRegistryConfigs.AZURE_CLIENT_SECRET,
                                                "azure-client-secret",
                                                SchemaRegistryConfigs.AZURE_TENANT_ID,
                                                "azure-tenant-id")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Schema registry provider [AZURE] does not support Protobuf schema evaluation for record key");

        ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
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
                                                "true",
                                                SchemaRegistryConfigs.URL,
                                                "http://localhost:8081",
                                                SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                                SchemaRegistryProvider.AZURE.toString(),
                                                SchemaRegistryConfigs.AZURE_CLIENT_ID,
                                                "azure-client-id",
                                                SchemaRegistryConfigs.AZURE_CLIENT_SECRET,
                                                "azure-client-secret",
                                                SchemaRegistryConfigs.AZURE_TENANT_ID,
                                                "azure-tenant-id")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Schema registry provider [AZURE] does not support Protobuf schema evaluation for record value");
    }

    @Test
    void shouldGetKvpPairsSeparator() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
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
    void shouldFailDueToInvalidKvpPairsSeparator(String delimiter) {
        Map<String, String> configs1 = new HashMap<>();
        configs1.put(RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR, delimiter);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs1));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.key.evaluator.kvp.pairs.separator]");

        Map<String, String> configs2 = new HashMap<>();
        configs2.put(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR, delimiter);

        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs2));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.value.evaluator.kvp.pairs.separator]");
    }

    @Test
    void shouldGetKvpValueSeparator() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
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
    void shouldFailDueToInvalidKvpSeparator(String delimiter) {
        Map<String, String> configs1 = new HashMap<>();
        configs1.put(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, delimiter);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs1));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.key.evaluator.kvp.key-value.separator]");

        Map<String, String> configs2 = new HashMap<>();
        configs2.put(RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, delimiter);

        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs2));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.value.evaluator.kvp.key-value.separator]");
    }

    @Test
    void shouldGetOverriddenGroupId() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(GROUP_ID, "lightstreamer-kafka-consumer-group");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getText(GROUP_ID)).isEqualTo("lightstreamer-kafka-consumer-group");
        assertThat(config.baseConsumerProps())
                .containsEntry(GROUP_ID_CONFIG, "lightstreamer-kafka-consumer-group");
    }

    @Test
    void shouldDefaultConsumerMode() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getConsumerMode()).isEqualTo(ConsumerMode.GROUP);
        assertThat(config.isManual()).isFalse();

        config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(), Map.of(CONSUMER_MODE, "GROUP"));
        assertThat(config.getConsumerMode()).isEqualTo(ConsumerMode.GROUP);
        assertThat(config.isManual()).isFalse();
    }

    @Test
    void shouldAcceptManualMode() {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(), Map.of(CONSUMER_MODE, "MANUAL"));
        assertThat(config.getConsumerMode()).isEqualTo(ConsumerMode.MANUAL);
        assertThat(config.isManual()).isTrue();
        assertThat(config.baseConsumerProps().containsKey(GROUP_ID_CONFIG)).isFalse();
    }

    @Test
    void shouldFailDueToInvalidConsumerMode() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(), Map.of(CONSUMER_MODE, "INVALID")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [consumer.mode]");
    }

    @Test
    void shouldFailDueToRegexEnabledInManualMode() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                CONSUMER_MODE,
                                                "MANUAL",
                                                MAP_REG_EX_ENABLE,
                                                "true")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Manual mode does not support regex topic matching. Parameter [map.regex.enable] must be set to [false] when [consumer.mode] is set to [MANUAL]");
    }

    @Test
    void shouldFailDueToPartitionsMappingWithGroupMode() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of("map.topic.from.partitions", "1,2,3")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Group mode does not support partition mappings. Parameter [map.topic.from.partitions] must be empty when [consumer.mode] is set to [GROUP]");
    }

    @Test
    void shouldFailDueToPartitionsMappingWithMissingTopic() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                "consumer.mode",
                                                "MANUAL",
                                                "map.not-mapped-topic1.from.partitions",
                                                "1,2,3",
                                                "map.not-mapped-topic2.from.partitions",
                                                "4-5")));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Partition mappings found for topics with no item mappings: [not-mapped-topic1, not-mapped-topic2]");
    }

    @Test
    void shouldFailDueToInvalidPartitionsMapping() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                ConnectorConfigProvider.minimalWith(
                                        adapterDir.toString(),
                                        Map.of(
                                                "consumer.mode",
                                                "MANUAL",
                                                "map.topic.from.partitions",
                                                "A-1")));
        assertThat(ce).hasMessageThat().isEqualTo("Partition range bounds must be integers: [A-1]");
    }

    @Test
    void shouldGetTopicMappingWithOneReference() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put("map.topic-test.to", "item-template.template1");
        ConnectorConfig cgg1 =
                ConnectorConfigProvider.minimalWith(adapterDir.toString(), updatedConfigs);

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
        ConnectorConfig cgg1 =
                ConnectorConfigProvider.minimalWith(adapterDir.toString(), updatedConfigs);

        List<TopicMappingConfig> topicMappings = cgg1.getTopicMappings();
        assertThat(topicMappings).hasSize(2);

        TopicMappingConfig tm1 = topicMappings.get(0);
        assertThat(tm1.topic()).isEqualTo("topic-test");
        assertThat(tm1.mappings()).containsExactly("item-template.template1", "item1", "item2");
    }

    @Test
    void shouldGetTopicMappingWithPartitions() {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put(CONSUMER_MODE, "MANUAL");
        updatedConfigs.put("map.topic-test.to", "item-template.template1");
        updatedConfigs.put("map.topic-test.from.partitions", "0-4,8-10,12,13");
        ConnectorConfig cgg1 =
                ConnectorConfigProvider.minimalWith(adapterDir.toString(), updatedConfigs);

        List<TopicMappingConfig> topicMappings = cgg1.getTopicMappings();
        assertThat(topicMappings).hasSize(2);

        TopicMappingConfig tm1 = topicMappings.get(0);
        assertThat(tm1.topic()).isEqualTo("topic-test");
        assertThat(tm1.mappings()).containsExactly("item-template.template1");
        assertThat(tm1.partitions()).containsExactly(0, 1, 2, 3, 4, 8, 9, 10, 12, 13);
    }

    @Test
    void shouldGetItemTemplateConfigs() {
        ConnectorConfig cgg1 = ConnectorConfigProvider.minimal(adapterDir.toString());

        var templateConfig = cgg1.getItemTemplateConfigs();
        assertThat(templateConfig.templates()).isEmpty();

        ConnectorConfig cgg2 =
                ConnectorConfigProvider.minimalWith(
                        adapterDir.toString(),
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
    void shouldGetMapRegEx() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.isMapRegExEnabled()).isFalse();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(MAP_REG_EX_ENABLE, "true");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isMapRegExEnabled()).isTrue();
    }

    @Test
    void shouldFailDueToInvalidMapRegExFlag() {
        Map<String, String> configs = new HashMap<>();
        configs.put(MAP_REG_EX_ENABLE, "t");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [map.regex.enable]");
    }

    @Test
    void shouldResolveSubscriptionMode() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getSubscriptionMode()).isEmpty();
        assertThat(config.getItemSnapshotMode()).isEqualTo(ItemSnapshotEnabledMode.NONE);

        // Hosts the configuration
        Map<String, String> updatedConfig;

        Map<String, ItemSnapshotEnabledMode> expectedSnapshotModes = new HashMap<>();
        expectedSnapshotModes.put("NONE", ItemSnapshotEnabledMode.NONE);
        expectedSnapshotModes.put("MERGE", ItemSnapshotEnabledMode.MERGE);
        expectedSnapshotModes.put("DISTINCT", ItemSnapshotEnabledMode.DISTINCT);

        Map<String, Mode> expectedSubscriptionModes = new HashMap<>();
        expectedSubscriptionModes.put("NONE", null);
        expectedSubscriptionModes.put("MERGE", Mode.MERGE);
        expectedSubscriptionModes.put("DISTINCT", Mode.DISTINCT);

        for (Map.Entry<String, ItemSnapshotEnabledMode> entry : expectedSnapshotModes.entrySet()) {
            String configuredItemSnapshotMode = entry.getKey();
            ItemSnapshotEnabledMode expectedSnapshotMode = entry.getValue();
            Mode expectedSubscriptionMode =
                    expectedSubscriptionModes.get(configuredItemSnapshotMode);

            updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.put(ITEM_SNAPSHOT_ENABLED_MODE, configuredItemSnapshotMode);
            config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
            assertThat(config.getItemSnapshotMode()).isEqualTo(expectedSnapshotMode);
            if (expectedSubscriptionMode == null) {
                assertThat(config.getSubscriptionMode()).isEmpty();
            } else {
                assertThat(config.getSubscriptionMode()).hasValue(expectedSubscriptionMode);
            }
        }

        // Test the COMMAND mode, which requires the field.key parameter to be set
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> {
                            Map<String, String> configs = new HashMap<>(standardParameters());
                            configs.put(ITEM_SNAPSHOT_ENABLED_MODE, "COMMAND");
                            ConnectorConfig.newConfig(adapterDir.toFile(), configs);
                        });
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Parameter [item.snapshot.enabled.mode] set to [COMMAND] requires [field.key] to be set");

        // Test the COMMAND mode with an invalid field.key parameter
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> {
                            Map<String, String> configs = new HashMap<>(standardParameters());
                            configs.put(ITEM_SNAPSHOT_ENABLED_MODE, "COMMAND");
                            configs.put("field.key", "");
                            ConnectorConfig.newConfig(adapterDir.toFile(), configs);
                        });
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [field.key]");

        Set<String> invalidExpressions = Set.of("#{VALUE}", "#{VALUE.value}");
        for (String invalidExpression : invalidExpressions) {
            ce =
                    assertThrows(
                            ConfigException.class,
                            () -> {
                                Map<String, String> configs = new HashMap<>(standardParameters());
                                configs.put(ITEM_SNAPSHOT_ENABLED_MODE, "COMMAND");
                                configs.put("field.key", invalidExpression);
                                ConnectorConfig.newConfig(adapterDir.toFile(), configs);
                            });
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Parameter [field.key] must be set to a constant expression referencing [KEY] when [item.snapshot.enabled.mode] is set to [COMMAND]");
        }

        // Test the COMMAND mode with a valid field.key parameter
        Set<String> validKeyExpressions = Set.of("#{KEY}", "#{KEY.value}");
        for (String validExpression : validKeyExpressions) {
            updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.put(ITEM_SNAPSHOT_ENABLED_MODE, "COMMAND");
            updatedConfig.put("field.key", validExpression);
            ConnectorConfig configWithCommandMode =
                    ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
            assertThat(configWithCommandMode.getItemSnapshotMode())
                    .isEqualTo(ItemSnapshotEnabledMode.COMMAND);
            assertThat(configWithCommandMode.getSubscriptionMode()).hasValue(Mode.COMMAND);
        }
    }

    static Stream<Arguments> itemSnapshotEnabledModeProvider() {
        return Stream.of(
                Arguments.of("NONE", ItemSnapshotEnabledMode.NONE, null),
                Arguments.of("MERGE", ItemSnapshotEnabledMode.MERGE, Mode.MERGE),
                Arguments.of("DISTINCT", ItemSnapshotEnabledMode.DISTINCT, Mode.DISTINCT));
    }

    @ParameterizedTest
    @MethodSource("itemSnapshotEnabledModeProvider")
    void shouldGetItemSnapshotEnabledMode(
            String modeString,
            ItemSnapshotEnabledMode expectedSnapshotMode,
            Mode expectedSubscriptionMode) {

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ITEM_SNAPSHOT_ENABLED_MODE, modeString);
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getItemSnapshotMode()).isEqualTo(expectedSnapshotMode);
        if (expectedSubscriptionMode == null) {
            assertThat(config.getSubscriptionMode()).isEmpty();
        } else {
            assertThat(config.getSubscriptionMode()).hasValue(expectedSubscriptionMode);
        }
    }

    @Test
    void shouldGetItemSnapshotDistinctLength() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getItemSnapshotDistinctLength()).isEqualTo(10);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ITEM_SNAPSHOT_DISTINCT_LENGTH, "20");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getItemSnapshotDistinctLength()).isEqualTo(20);

        updatedConfig.put(ITEM_SNAPSHOT_DISTINCT_LENGTH, "invalid_length");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [item.snapshot.distinct.length]");
    }

    @Test
    void shouldGetItemSnapshotMaxIdleSeconds() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getItemSnapshotMaxIdleSeconds()).isEqualTo(0);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ITEM_SNAPSHOT_MAX_IDLE_SECONDS, "400");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getItemSnapshotMaxIdleSeconds()).isEqualTo(400);

        String[] invalidValues = {"invalid_ttl", "-1"};
        for (String invalidValue : invalidValues) {
            updatedConfig.put(ITEM_SNAPSHOT_MAX_IDLE_SECONDS, invalidValue);
            ConfigException ce =
                    assertThrows(
                            ConfigException.class,
                            () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Specify a valid value for parameter [item.snapshot.max.idle.seconds]");
        }
    }

    @Test
    void shouldFailDueToInvalidItemSnapshotEnabledMode() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ITEM_SNAPSHOT_ENABLED_MODE, "invalid_snapshot_mode");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [item.snapshot.enabled.mode]");
    }

    @Test
    void shouldGetFieldsSkipFailedMapping() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.isFieldsSkipFailedMappingEnabled()).isFalse();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(FIELDS_SKIP_FAILED_MAPPING_ENABLE, "true");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isFieldsSkipFailedMappingEnabled()).isTrue();
    }

    @Test
    void shouldGetFieldsMapNonScalarValues() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.isFieldsMapNonScalarValuesEnabled()).isFalse();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(FIELDS_MAP_NON_SCALAR_VALUES_ENABLE, "true");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isFieldsMapNonScalarValuesEnabled()).isTrue();
    }

    @Test
    void shouldFailDueToFieldsSkipFailedMapping() {
        Map<String, String> configs = new HashMap<>();
        configs.put(FIELDS_SKIP_FAILED_MAPPING_ENABLE, "t");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [fields.skip.failed.mapping.enable]");
    }

    @Test
    void shouldFailDueToFieldsMapNonScalarValues() {
        Map<String, String> configs = new HashMap<>();
        configs.put(FIELDS_MAP_NON_SCALAR_VALUES_ENABLE, "t");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [fields.map.non.scalar.values.enable]");
    }

    @Test
    void shouldFailDueToInvalidRegularExpressionInTopicMapping() {
        Map<String, String> configs = new HashMap<>();
        configs.put(MAP_REG_EX_ENABLE, "true");
        configs.put("map.topic_\\d.to", "item"); // Valid regular expression
        configs.put("map.\\k.to", "item"); // Invalid regular expression

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfigProvider.minimalWith(adapterDir.toString(), configs));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid regular expression for parameter [map.\\k.to]");
    }

    @Test
    void shouldGetFieldConfigs() {
        ConnectorConfig cgg = ConnectorConfigProvider.minimal(adapterDir.toString());
        FieldConfigs fieldConfigs = cgg.getFieldConfigs();
        assertThat(fieldConfigs.namedFieldsExpressions()).hasSize(1);
        assertThat(fieldConfigs.namedFieldsExpressions().get("fieldName1").toString())
                .isEqualTo("VALUE");
    }

    @Test
    void shouldGetHostList() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getHostsList(BOOTSTRAP_SERVERS)).isEqualTo("server:8080,server:8081");
    }

    @Test
    void shouldGetDefaultText() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getText(ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getText(DATA_ADAPTER_NAME)).isEqualTo("CONNECTOR");
    }

    @Test
    void shouldGetEnabled() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.isEnabled()).isTrue();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ENABLE, "false");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isEnabled()).isFalse();
    }

    @Test
    void shouldOverrideConsumeEventsFrom() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_FROM, "EARLIEST");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordConsumeFrom()).isEqualTo(EARLIEST);
        assertThat(config.baseConsumerProps())
                .containsAtLeast(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    void shouldGetEncryptionEnabled() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.isEncryptionEnabled()).isFalse();
    }

    @Test
    void shouldGetAuthenticationEnabled() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.isAuthenticationEnabled()).isFalse();
    }

    @Test
    void shouldGetDefaultEvaluator() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
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
    void shouldGetErrorStrategy() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getRecordExtractionErrorHandlingStrategy())
                .isEqualTo(IGNORE_AND_CONTINUE);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(
                RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, FORCE_UNSUBSCRIPTION.toString());
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordExtractionErrorHandlingStrategy())
                .isEqualTo(FORCE_UNSUBSCRIPTION);

        updatedConfig.put(ITEM_SNAPSHOT_ENABLED_MODE, "DISTINCT");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordExtractionErrorHandlingStrategy())
                .isEqualTo(IGNORE_AND_CONTINUE);
    }

    @Test
    void shouldFailDueToInvalidErrorStrategyType() {
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
    void shouldGetRecordConsumeWithOrderStrategy() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
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
    void shouldFailDueToInvalidOrderStrategyType() {
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
    void shouldGetRecordConsumeWithThreadsNumber() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getRecordConsumeWithNumThreads()).isEqualTo(1);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_NUM_THREADS, "10");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordConsumeWithNumThreads()).isEqualTo(10);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-2", "abc", "0.5"})
    void shouldFailDueToInvalidRecordConsumeWithThreadsNumber(String invalidThreadsNumber) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_NUM_THREADS, invalidThreadsNumber);
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
    void shouldGetOverriddenMaxPollRecords() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_MAX_POLL_RECORDS, "200");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.baseConsumerProps())
                .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "0.4", "abc"})
    void shouldFailDueToInvalidMaxPollRecords(String invalidMaxPollRecords) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_MAX_POLL_RECORDS, invalidMaxPollRecords);
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.consume.with.max.poll.records]");
    }

    @Test
    void shouldGetOverriddenSessionTimeoutMs() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS, "35000");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.baseConsumerProps())
                .containsEntry(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "35000");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.4", "abc"})
    void shouldFailDueToInvalidSessionTimeoutMs(String invalidSessionTimeoutMs) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS, invalidSessionTimeoutMs);
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.consume.with.session.timeout.ms]");
    }

    @Test
    void shouldGetOverriddenMaxPollIntervalMs() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS, "35000");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.baseConsumerProps())
                .containsEntry(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "35000");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc"})
    void shouldFailDueToInvalidMaxPollIntervalMs(String invalidMaxPollIntervalMs) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS, invalidMaxPollIntervalMs);
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [record.consume.with.max.poll.interval.ms]");
    }

    @Test
    void shouldManageSchemaFiles() {
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
    void shouldNoGetNonExistingNonRequiredInt() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getNonNegativeInt(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG)).isNull();
        assertThat(config.getNonNegativeInt(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG)).isNull();
        assertThat(config.getNonNegativeInt(CONSUMER_FETCH_MAX_BYTES_CONFIG)).isNull();
        assertThat(config.getNonNegativeInt(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG)).isNull();
        assertThat(config.getNonNegativeInt(CONSUMER_FETCH_MIN_BYTES_CONFIG)).isNull();
        assertThat(config.getInt(CONSUMER_HEARTBEAT_INTERVAL_MS)).isNull();
    }

    @Test
    void shouldNotAccessEncryptionSettings() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        assertThat(config.isEncryptionEnabled()).isFalse();

        List<Executable> executables =
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
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo("Encryption is not enabled. Check parameter [encryption.enable]");
        }
    }

    @Test
    void shouldSpecifyEncryptionParametersWhenRequired() {
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
    void shouldGetDefaultEncryptionSettings() {
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

        List<Executable> executables =
                List.of(
                        () -> config.keystorePath(),
                        () -> config.keystorePassword(),
                        () -> config.keystoreType(),
                        () -> config.keyPassword());
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Key store is not enabled. Check parameter [encryption.keystore.enable]");
        }
    }

    @Test
    void shouldOverrideEncryptionSettings() {
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
    void shouldSpecifyRequiredKeystoreParameters() {
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
    void shouldGetDefaultKeystoreSettings() {
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
    void shouldOverrideKeystoreSettings() {
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
    void shouldSpecifyAuthenticationRequiredParametersWithDefaultPlain() {
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
    void shouldSpecifyAuthenticationRequiredParametersWithSCRAM(String saslMechanism) {
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
    void shouldNotAccessAuthenticationSettings() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        assertThat(config.isAuthenticationEnabled()).isFalse();

        List<Executable> executables =
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
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Authentication is not enabled. Check parameter [authentication.enable]");
        }
    }

    @Test
    void shouldGetDefaultAuthenticationSettings() {
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
    void shouldOverrideAuthenticationSettings() {
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
    void shouldOverrideAuthenticationSettingsWithIam() {
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
    void shouldSpecifyGssapiAuthenticationRequiredParameters() {
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
    void shouldGetDefaultGssapiAuthenticationSettings() {
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
    void shouldOverrideGssapiAuthenticationSettings() {
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
    void shouldOverrideGssapiAuthenticationSettingsWithTicketCache() {
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
    void shouldNotValidateWhenKeyTabIsNotSpecified() {
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
    void shouldNotValidateWhenPrincipalIsNotSpecifiedAndNotUseTicketCache() {
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
    void shouldSpecifyRequiredParamsForConfluentSchemaRegistry() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");

        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, null);
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [schema.registry.provider]");

        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "INVALID");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [schema.registry.provider]");

        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "CONFLUENT");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryEnabled()).isTrue();
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://localhost:8080");
        assertThat(config.schemaRegistryProvider()).isEqualTo(SchemaRegistryProvider.CONFLUENT);
    }

    @Test
    void shouldSpecifyRequiredParamsForAzureSchemaRegistry() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");

        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, null);
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [schema.registry.provider]");

        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "INVALID");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [schema.registry.provider]");

        updatedConfig.put(
                SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                SchemaRegistryProvider.AZURE.toString());
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [schema.registry.azure.tenant.id]");

        updatedConfig.put(SchemaRegistryConfigs.AZURE_TENANT_ID, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [schema.registry.azure.tenant.id]");

        updatedConfig.put(
                SchemaRegistryConfigs.AZURE_TENANT_ID, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [schema.registry.azure.client.id]");

        updatedConfig.put(SchemaRegistryConfigs.AZURE_CLIENT_ID, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Specify a valid value for parameter [schema.registry.azure.client.id]");

        updatedConfig.put(SchemaRegistryConfigs.AZURE_CLIENT_ID, "client-id");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo("Missing required parameter [schema.registry.azure.client.secret]");

        updatedConfig.put(SchemaRegistryConfigs.AZURE_CLIENT_SECRET, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.azure.client.secret]");

        updatedConfig.put(SchemaRegistryConfigs.AZURE_CLIENT_SECRET, "client-secret");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryEnabled()).isTrue();
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://localhost:8080");
        assertThat(config.schemaRegistryProvider()).isEqualTo(SchemaRegistryProvider.AZURE);
        assertThat(config.azureTenantId()).isEqualTo("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        assertThat(config.azureClientId()).isEqualTo("client-id");
        assertThat(config.azureClientSecret()).isEqualTo("client-secret");
    }

    @Test
    void shouldNotAccessSchemaRegistrySettings() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        assertThat(config.isSchemaRegistryEnabled()).isFalse();
        List<Executable> executables =
                List.of(
                        () -> config.isConfluentSchemaRegistryEncryptionEnabled(),
                        () -> config.confluentSchemaRegistryEnabledProtocols(),
                        () -> config.confluentSchemaRegistryEnabledProtocolsAsStr(),
                        () -> config.confluentSchemaRegistrySslProtocol(),
                        () -> config.confluentSchemaRegistryTruststoreType(),
                        () -> config.confluentSchemaRegistryTruststorePath(),
                        () -> config.confluentSchemaRegistryTruststorePassword(),
                        () -> config.isConfluentSchemaRegistryHostNameVerificationEnabled(),
                        () -> config.confluentSchemaRegistryCipherSuites(),
                        () -> config.confluentSchemaRegistryCipherSuitesAsStr(),
                        () -> config.confluentSchemaRegistrySslProvider(),
                        () -> config.isSchemaRegistryBasicAuthenticationEnabled(),
                        () -> config.azureClientId(),
                        () -> config.azureTenantId(),
                        () -> config.azureClientSecret());
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Neither parameter [record.key.evaluator.schema.registry.enable] nor parameter [record.value.evaluator.schema.registry.enable] are enabled");
        }
    }

    @Test
    void shouldNotAccessAzureSchemaRegistrySettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://localhost:8080");
        List<Executable> executables =
                List.of(
                        () -> config.azureTenantId(),
                        () -> config.azureClientId(),
                        () -> config.azureClientSecret());

        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo("Parameter [schema.registry.provider] is not set to [AZURE]");
        }
    }

    @Test
    void shouldNotAccessConfluentSchemaRegistrySettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(URL, "http://localhost:8080");
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(
                SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                SchemaRegistryProvider.AZURE.toString());
        updatedConfig.put(
                SchemaRegistryConfigs.AZURE_TENANT_ID, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        updatedConfig.put(SchemaRegistryConfigs.AZURE_CLIENT_ID, "client-id");
        updatedConfig.put(SchemaRegistryConfigs.AZURE_CLIENT_SECRET, "client-secret");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://localhost:8080");

        List<Executable> executables =
                List.of(
                        () -> config.isConfluentSchemaRegistryEncryptionEnabled(),
                        () -> config.confluentSchemaRegistryEnabledProtocols(),
                        () -> config.confluentSchemaRegistryEnabledProtocolsAsStr(),
                        () -> config.confluentSchemaRegistrySslProtocol(),
                        () -> config.confluentSchemaRegistryTruststoreType(),
                        () -> config.confluentSchemaRegistryTruststorePath(),
                        () -> config.confluentSchemaRegistryTruststorePassword(),
                        () -> config.isConfluentSchemaRegistryHostNameVerificationEnabled(),
                        () -> config.confluentSchemaRegistryCipherSuites(),
                        () -> config.confluentSchemaRegistryCipherSuitesAsStr(),
                        () -> config.confluentSchemaRegistrySslProvider(),
                        () -> config.isSchemaRegistryBasicAuthenticationEnabled());
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo("Parameter [schema.registry.provider] is not set to [CONFLUENT]");
        }
    }

    @Test
    void shouldNotAccessConfluentSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://localhost:8080");
        assertThat(config.isConfluentSchemaRegistryEncryptionEnabled()).isFalse();

        List<Executable> executables =
                List.of(
                        () -> config.confluentSchemaRegistryEnabledProtocols(),
                        () -> config.confluentSchemaRegistryEnabledProtocolsAsStr(),
                        () -> config.confluentSchemaRegistrySslProtocol(),
                        () -> config.confluentSchemaRegistryTruststoreType(),
                        () -> config.confluentSchemaRegistryTruststorePath(),
                        () -> config.confluentSchemaRegistryTruststorePassword(),
                        () -> config.isConfluentSchemaRegistryHostNameVerificationEnabled(),
                        () -> config.confluentSchemaRegistryCipherSuites(),
                        () -> config.confluentSchemaRegistryCipherSuitesAsStr(),
                        () -> config.confluentSchemaRegistrySslProvider());
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo("Parameter [schema.registry.url] is not set to https protocol");
        }
    }

    @Test
    void shouldGetDefaultConfluentSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.schemaRegistryUrl()).isEqualTo("https://localhost:8080");

        assertThat(config.isSchemaRegistryEnabled()).isTrue();
        assertThat(config.confluentSchemaRegistryEnabledProtocols())
                .containsExactly(TLSv12, TLSv13);
        assertThat(config.confluentSchemaRegistryEnabledProtocolsAsStr())
                .isEqualTo("TLSv1.2,TLSv1.3");
        assertThat(config.confluentSchemaRegistrySslProtocol().toString()).isEqualTo("TLSv1.3");
        assertThat(config.confluentSchemaRegistryTruststoreType().toString()).isEqualTo("JKS");
        assertThat(config.confluentSchemaRegistryTruststorePath()).isNull();
        assertThat(config.confluentSchemaRegistryTruststorePassword()).isNull();
        assertThat(config.isConfluentSchemaRegistryHostNameVerificationEnabled()).isFalse();
        assertThat(config.confluentSchemaRegistryCipherSuites()).isEmpty();
        assertThat(config.confluentSchemaRegistryCipherSuitesAsStr()).isNull();
        assertThat(config.confluentSchemaRegistrySslProvider()).isNull();
        assertThat(config.isConfluentSchemaRegistryKeystoreEnabled()).isFalse();

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

        List<Executable> executables =
                List.of(
                        () -> config.confluentSchemaRegistryKeystorePath(),
                        () -> config.confluentSchemaRegistryKeystorePassword(),
                        () -> config.confluentSchemaRegistryKeystoreType(),
                        () -> config.schemaRegistryKeyPassword());
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Parameter [schema.registry.confluent.encryption.keystore.enable] is not enabled");
        }
    }

    @Test
    void shouldOverrideConfluentSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "CONFLUENT");
        updatedConfig.put(CONFLUENT_TRUSTSTORE_PATH, trustStoreFile.getFileName().toString());
        updatedConfig.put(CONFLUENT_TRUSTSTORE_PASSWORD, "truststore-password");
        updatedConfig.put(CONFLUENT_SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        updatedConfig.put(CONFLUENT_SSL_PROTOCOL, "TLSv1.2");
        updatedConfig.put(CONFLUENT_TRUSTSTORE_TYPE, "PKCS12");
        updatedConfig.put(
                CONFLUENT_SSL_CIPHER_SUITES,
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        updatedConfig.put(CONFLUENT_HOSTNAME_VERIFICATION_ENABLE, "true");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isConfluentSchemaRegistryEncryptionEnabled()).isTrue();
        assertThat(config.confluentSchemaRegistryEnabledProtocols()).containsExactly(TLSv12);
        assertThat(config.confluentSchemaRegistryEnabledProtocolsAsStr()).isEqualTo("TLSv1.2");
        assertThat(config.confluentSchemaRegistrySslProtocol().toString()).isEqualTo("TLSv1.2");
        assertThat(config.confluentSchemaRegistryTruststoreType().toString()).isEqualTo("PKCS12");
        assertThat(config.confluentSchemaRegistryTruststorePath())
                .isEqualTo(trustStoreFile.toString());
        assertThat(config.confluentSchemaRegistryTruststorePassword())
                .isEqualTo("truststore-password");
        assertThat(config.isConfluentSchemaRegistryHostNameVerificationEnabled()).isTrue();
        assertThat(config.confluentSchemaRegistryCipherSuites())
                .containsExactly(
                        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.confluentSchemaRegistryCipherSuitesAsStr())
                .isEqualTo("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.confluentSchemaRegistrySslProvider()).isNull();

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
    void shouldGetDefaultConfluentSchemaRegistryKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "CONFLUENT");

        updatedConfig.put(CONFLUENT_KEYSTORE_ENABLE, "true");
        updatedConfig.put(CONFLUENT_KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        updatedConfig.put(CONFLUENT_KEYSTORE_PASSWORD, "keystore-password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isConfluentSchemaRegistryKeystoreEnabled()).isTrue();
        assertThat(config.confluentSchemaRegistryKeystorePath()).isEqualTo(keyStoreFile.toString());
        assertThat(config.confluentSchemaRegistryKeystoreType().toString()).isEqualTo("JKS");
        assertThat(config.confluentSchemaRegistryKeystorePassword()).isEqualTo("keystore-password");
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
    void shouldOverrideConfluentSchemaRegistryKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "https://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "CONFLUENT");
        updatedConfig.put(CONFLUENT_KEYSTORE_ENABLE, "true");
        updatedConfig.put(CONFLUENT_KEYSTORE_TYPE, "PKCS12");
        updatedConfig.put(CONFLUENT_KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        updatedConfig.put(CONFLUENT_KEYSTORE_PASSWORD, "keystore-password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.schemaRegistryProvider()).isEqualTo(SchemaRegistryProvider.CONFLUENT);
        assertThat(config.isConfluentSchemaRegistryKeystoreEnabled()).isTrue();
        assertThat(config.confluentSchemaRegistryKeystoreType().toString()).isEqualTo("PKCS12");

        updatedConfig.put(CONFLUENT_KEY_PASSWORD, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.confluent.encryption.keystore.key.password]");

        updatedConfig.put(CONFLUENT_KEY_PASSWORD, "key-password");
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
    void shouldNotAccessConfluentSchemaRegistryBasicAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "CONFLUENT");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryBasicAuthenticationEnabled()).isFalse();

        List<Executable> executables =
                List.of(
                        () -> config.confluentSchemaRegistryBasicAuthenticationUserName(),
                        () -> config.confluentSchemaRegistryBasicAuthenticationPassword());
        for (Executable executable : executables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Parameter [schema.registry.confluent.basic.authentication.enable] is not enabled");
        }
    }

    @Test
    void shouldGetConfluentSchemaRegistryBasicAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(URL, "http://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER, "CONFLUENT");
        updatedConfig.put(SchemaRegistryConfigs.CONFLUENT_ENABLE_BASIC_AUTHENTICATION, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [schema.registry.confluent.basic.authentication.username]");

        updatedConfig.put(CONFLUENT_BASIC_AUTHENTICATION_USER_NAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.confluent.basic.authentication.username]");

        updatedConfig.put(CONFLUENT_BASIC_AUTHENTICATION_USER_NAME, "username");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [schema.registry.confluent.basic.authentication.password]");

        updatedConfig.put(CONFLUENT_BASIC_AUTHENTICATION_USER_PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.confluent.basic.authentication.password]");

        updatedConfig.put(CONFLUENT_BASIC_AUTHENTICATION_USER_PASSWORD, "password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryBasicAuthenticationEnabled()).isTrue();
        assertThat(config.confluentSchemaRegistryBasicAuthenticationUserName())
                .isEqualTo("username");
        assertThat(config.confluentSchemaRegistryBasicAuthenticationPassword())
                .isEqualTo("password");

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        BASIC_AUTH_CREDENTIALS_SOURCE,
                        "USER_INFO",
                        USER_INFO_CONFIG,
                        "username:password");
    }
}
