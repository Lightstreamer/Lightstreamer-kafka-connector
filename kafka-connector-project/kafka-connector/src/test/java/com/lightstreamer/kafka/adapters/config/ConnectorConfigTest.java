
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol.TLSv12;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol.TLSv13;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordComsumeFrom;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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

    private Path keySchemaFile;
    private Path valueSchemaFile;

    private Path trustStoreFile;
    private Path keyStoreFile;

    private Path keyTabFile;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        keySchemaFile = Files.createTempFile(adapterDir, "key-schema-", ".avsc");
        valueSchemaFile = Files.createTempFile(adapterDir, "value-schema-", ".avsc");
        trustStoreFile = Files.createTempFile(adapterDir, "truststore", ".jks");
        keyStoreFile = Files.createTempFile(adapterDir, "keystore", ".jks");
        keyTabFile = Files.createTempFile(adapterDir, "keytabFile", ".keytab");
    }

    @Test
    public void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = ConnectorConfig.configSpec();

        ConfParameter adapterDir = configSpec.getParameter(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDir.name()).isEqualTo(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDir.required()).isTrue();
        assertThat(adapterDir.multiple()).isFalse();
        assertThat(adapterDir.mutable()).isTrue();
        assertThat(adapterDir.defaultValue()).isNull();
        assertThat(adapterDir.type()).isEqualTo(ConfType.DIRECTORY);

        ConfParameter enabled = configSpec.getParameter(ConnectorConfig.ENABLE);
        assertThat(enabled.name()).isEqualTo(ConnectorConfig.ENABLE);
        assertThat(enabled.required()).isFalse();
        assertThat(enabled.multiple()).isFalse();
        assertThat(enabled.mutable()).isTrue();
        assertThat(enabled.defaultValue()).isEqualTo("true");
        assertThat(enabled.type()).isEqualTo(ConfType.BOOL);

        ConfParameter adapterConfId = configSpec.getParameter(ConnectorConfig.ADAPTERS_CONF_ID);
        assertThat(adapterConfId.name()).isEqualTo(ConnectorConfig.ADAPTERS_CONF_ID);
        assertThat(adapterConfId.required()).isTrue();
        assertThat(adapterConfId.multiple()).isFalse();
        assertThat(adapterConfId.mutable()).isTrue();
        assertThat(adapterConfId.defaultValue()).isNull();
        assertThat(adapterConfId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter dataAdapterName = configSpec.getParameter(ConnectorConfig.DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.name()).isEqualTo(ConnectorConfig.DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.required()).isTrue();
        assertThat(dataAdapterName.multiple()).isFalse();
        assertThat(dataAdapterName.mutable()).isTrue();
        assertThat(dataAdapterName.defaultValue()).isNull();
        assertThat(dataAdapterName.type()).isEqualTo(ConfType.TEXT);

        ConfParameter bootStrapServers = configSpec.getParameter(ConnectorConfig.BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.name()).isEqualTo(ConnectorConfig.BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.required()).isTrue();
        assertThat(bootStrapServers.multiple()).isFalse();
        assertThat(bootStrapServers.mutable()).isTrue();
        assertThat(bootStrapServers.defaultValue()).isNull();
        assertThat(bootStrapServers.type()).isEqualTo(ConfType.HOST_LIST);

        ConfParameter groupId = configSpec.getParameter(ConnectorConfig.GROUP_ID);
        assertThat(groupId.name()).isEqualTo(ConnectorConfig.GROUP_ID);
        assertThat(groupId.required()).isFalse();
        assertThat(groupId.multiple()).isFalse();
        assertThat(groupId.mutable()).isTrue();
        assertThat(groupId.defaultValue()).isNotNull();
        assertThat(groupId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyEvaluatorType =
                configSpec.getParameter(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.name()).isEqualTo(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.required()).isFalse();
        assertThat(keyEvaluatorType.multiple()).isFalse();
        assertThat(keyEvaluatorType.mutable()).isTrue();
        assertThat(keyEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(keyEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter keySchemaFile =
                configSpec.getParameter(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaFile.name())
                .isEqualTo(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaFile.required()).isFalse();
        assertThat(keySchemaFile.multiple()).isFalse();
        assertThat(keySchemaFile.mutable()).isTrue();
        assertThat(keySchemaFile.defaultValue()).isNull();
        assertThat(keySchemaFile.type()).isEqualTo(ConfType.FILE);

        ConfParameter schemaRegistryEnabledForKey =
                configSpec.getParameter(
                        ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForKey.name())
                .isEqualTo(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForKey.required()).isFalse();
        assertThat(schemaRegistryEnabledForKey.multiple()).isFalse();
        assertThat(schemaRegistryEnabledForKey.mutable()).isTrue();
        assertThat(schemaRegistryEnabledForKey.defaultValue()).isEqualTo("false");
        assertThat(schemaRegistryEnabledForKey.type()).isEqualTo(ConfType.BOOL);

        ConfParameter valueEvaluatorType =
                configSpec.getParameter(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.name())
                .isEqualTo(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.required()).isFalse();
        assertThat(valueEvaluatorType.multiple()).isFalse();
        assertThat(valueEvaluatorType.mutable()).isTrue();
        assertThat(valueEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(valueEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter valueSchemaFile =
                configSpec.getParameter(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaFile.name())
                .isEqualTo(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaFile.required()).isFalse();
        assertThat(valueSchemaFile.multiple()).isFalse();
        assertThat(valueSchemaFile.mutable()).isTrue();
        assertThat(valueSchemaFile.defaultValue()).isNull();
        assertThat(valueSchemaFile.type()).isEqualTo(ConfType.FILE);

        ConfParameter schemaRegistryEnabledForValue =
                configSpec.getParameter(
                        ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForValue.name())
                .isEqualTo(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
        assertThat(schemaRegistryEnabledForValue.required()).isFalse();
        assertThat(schemaRegistryEnabledForValue.multiple()).isFalse();
        assertThat(schemaRegistryEnabledForValue.mutable()).isTrue();
        assertThat(schemaRegistryEnabledForValue.defaultValue()).isEqualTo("false");
        assertThat(schemaRegistryEnabledForValue.type()).isEqualTo(ConfType.BOOL);

        ConfParameter itemTemplate = configSpec.getParameter(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.name()).isEqualTo(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.required()).isFalse();
        assertThat(itemTemplate.multiple()).isTrue();
        assertThat(itemTemplate.suffix()).isNull();
        assertThat(itemTemplate.mutable()).isTrue();
        ;
        assertThat(itemTemplate.defaultValue()).isNull();
        assertThat(itemTemplate.type()).isEqualTo(ConfType.TEXT);

        ConfParameter topicMapping = configSpec.getParameter(ConnectorConfig.TOPIC_MAPPING);
        assertThat(topicMapping.name()).isEqualTo(ConnectorConfig.TOPIC_MAPPING);
        assertThat(topicMapping.required()).isTrue();
        assertThat(topicMapping.multiple()).isTrue();
        assertThat(topicMapping.suffix()).isEqualTo("to");
        assertThat(topicMapping.mutable()).isTrue();
        assertThat(topicMapping.defaultValue()).isNull();
        assertThat(topicMapping.type()).isEqualTo(ConfType.TEXT_LIST);

        ConfParameter fieldMapping = configSpec.getParameter(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.name()).isEqualTo(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.required()).isTrue();
        assertThat(fieldMapping.multiple()).isTrue();
        assertThat(fieldMapping.suffix()).isNull();
        assertThat(fieldMapping.mutable()).isTrue();
        assertThat(fieldMapping.defaultValue()).isNull();
        assertThat(fieldMapping.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemInfoName = configSpec.getParameter(ConnectorConfig.ITEM_INFO_NAME);
        assertThat(itemInfoName.name()).isEqualTo(ConnectorConfig.ITEM_INFO_NAME);
        assertThat(itemInfoName.required()).isFalse();
        assertThat(itemInfoName.multiple()).isFalse();
        assertThat(itemInfoName.mutable()).isTrue();
        assertThat(itemInfoName.defaultValue()).isEqualTo("INFO");
        assertThat(itemInfoName.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemInfoField = configSpec.getParameter(ConnectorConfig.ITEM_INFO_FIELD);
        assertThat(itemInfoField.name()).isEqualTo(ConnectorConfig.ITEM_INFO_FIELD);
        assertThat(itemInfoField.required()).isFalse();
        assertThat(itemInfoField.multiple()).isFalse();
        assertThat(itemInfoField.mutable()).isTrue();
        assertThat(itemInfoField.defaultValue()).isEqualTo("MSG");
        assertThat(itemInfoField.type()).isEqualTo(ConfType.TEXT);

        ConfParameter enableAutoCommit =
                configSpec.getParameter(ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.name())
                .isEqualTo(ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.required()).isFalse();
        assertThat(enableAutoCommit.multiple()).isFalse();
        assertThat(enableAutoCommit.mutable()).isFalse();
        assertThat(enableAutoCommit.defaultValue()).isEqualTo("false");
        assertThat(enableAutoCommit.type()).isEqualTo(ConfType.BOOL);

        ConfParameter clientId = configSpec.getParameter(ConnectorConfig.CONSUMER_CLIENT_ID);
        assertThat(clientId.name()).isEqualTo(ConnectorConfig.CONSUMER_CLIENT_ID);
        assertThat(clientId.required()).isFalse();
        assertThat(clientId.multiple()).isFalse();
        assertThat(clientId.mutable()).isFalse();
        assertThat(clientId.defaultValue()).isEqualTo("");
        assertThat(clientId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter consumeEventsFrom =
                configSpec.getParameter(ConnectorConfig.RECORD_CONSUME_FROM);
        assertThat(consumeEventsFrom.name()).isEqualTo(ConnectorConfig.RECORD_CONSUME_FROM);
        assertThat(consumeEventsFrom.required()).isFalse();
        assertThat(consumeEventsFrom.multiple()).isFalse();
        assertThat(consumeEventsFrom.mutable()).isTrue();
        assertThat(consumeEventsFrom.defaultValue()).isEqualTo("LATEST");
        assertThat(consumeEventsFrom.type()).isEqualTo(ConfType.CONSUME_FROM);

        ConfParameter encryptionEnabed = configSpec.getParameter(ConnectorConfig.ENCYRPTION_ENABLE);
        assertThat(encryptionEnabed.name()).isEqualTo(ConnectorConfig.ENCYRPTION_ENABLE);
        assertThat(encryptionEnabed.required()).isFalse();
        assertThat(encryptionEnabed.multiple()).isFalse();
        assertThat(encryptionEnabed.mutable()).isTrue();
        assertThat(encryptionEnabed.defaultValue()).isEqualTo("false");
        assertThat(encryptionEnabed.type()).isEqualTo(ConfType.BOOL);

        ConfParameter authenticationEnabled =
                configSpec.getParameter(ConnectorConfig.AUTHENTICATION_ENABLE);
        assertThat(authenticationEnabled.name()).isEqualTo(ConnectorConfig.AUTHENTICATION_ENABLE);
        assertThat(authenticationEnabled.required()).isFalse();
        assertThat(authenticationEnabled.multiple()).isFalse();
        assertThat(authenticationEnabled.mutable()).isTrue();
        assertThat(authenticationEnabled.defaultValue()).isEqualTo("false");
        assertThat(authenticationEnabled.type()).isEqualTo(ConfType.BOOL);
    }

    private Map<String, String> standardParameters() {
        Map<String, String> standardParams = new HashMap<>();
        standardParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        standardParams.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "STRING");
        standardParams.put(
                ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                valueSchemaFile.getFileName().toString());
        standardParams.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "JSON");
        standardParams.put(
                ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                keySchemaFile.getFileName().toString());
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
        standardParams.put(ConnectorConfig.CONSUMER_HEARTBEAT_INTERVAL_MS, "600");
        standardParams.put(ConnectorConfig.CONSUMER_MAX_POLL_RECORDS, "700");
        standardParams.put(ConnectorConfig.CONSUMER_SESSION_TIMEOUT_MS, "800");
        standardParams.put(ConnectorConfig.CONSUMER_MAX_POLL_INTERVAL_MS, "2000"); // Unmodifiable
        standardParams.put(ConnectorConfig.CONSUMER_METADATA_MAX_AGE_CONFIG, "250"); // Unmodifiable
        standardParams.put("item-template.template1", "template1-#{v=VALUE}");
        standardParams.put("item-template.template2", "template2-#{v=OFFSET}");
        standardParams.put("map.topic1.to", "template1");
        standardParams.put("map.topic2.to", "template2");
        standardParams.put("field.fieldName1", "#{VALUE.bar}");
        return standardParams;
    }

    private Map<String, String> encryptionParameters() {
        Map<String, String> encryptionParams = new HashMap<>();
        encryptionParams.put(ConnectorConfig.ENCYRPTION_ENABLE, "true");
        return encryptionParams;
    }

    private Map<String, String> kesytoreParameters() {
        Map<String, String> keystoreParams = new HashMap<>();
        keystoreParams.put(EncryptionConfigs.ENABLE_MTLS, "true");
        keystoreParams.put(EncryptionConfigs.KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        return keystoreParams;
    }

    private Map<String, String> authenticationParameters() {
        Map<String, String> authParams = new HashMap<>();
        authParams.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");
        authParams.put(BrokerAuthenticationConfigs.USERNAME, "sasl-username");
        authParams.put(BrokerAuthenticationConfigs.PASSWORD, "sasl-password");
        return authParams;
    }

    @Test
    public void shouldSpecifyRequiredParams() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> new ConnectorConfig(Collections.emptyMap()));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(ConnectorConfig.ADAPTERS_CONF_ID));

        Map<String, String> params = new HashMap<>();

        params.put(ConnectorConfig.ADAPTERS_CONF_ID, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(ConnectorConfig.ADAPTERS_CONF_ID));

        params.put(ConnectorConfig.ADAPTERS_CONF_ID, "adapters_conf_id");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [%s]".formatted(ConnectorConfig.ADAPTER_DIR));

        params.put(ConnectorConfig.ADAPTER_DIR, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(ConnectorConfig.ADAPTER_DIR));

        params.put(ConnectorConfig.ADAPTER_DIR, "non-existing-directory");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Not found directory [non-existing-directory] specified in [%s]"
                                .formatted(ConnectorConfig.ADAPTER_DIR));

        params.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(ConnectorConfig.DATA_ADAPTER_NAME));

        params.put(ConnectorConfig.DATA_ADAPTER_NAME, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(ConnectorConfig.DATA_ADAPTER_NAME));

        params.put(ConnectorConfig.DATA_ADAPTER_NAME, "data_provider_name");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(ConnectorConfig.BOOTSTRAP_SERVERS));

        params.put(ConnectorConfig.BOOTSTRAP_SERVERS, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(ConnectorConfig.BOOTSTRAP_SERVERS));

        // Trailing "," not allowed
        params.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(ConnectorConfig.BOOTSTRAP_SERVERS));

        params.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify at least one parameter [map.<...>.to]");

        params.put("map.to", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid parameter [map.<...>.to]");
        params.remove("map.to");

        params.put("map.topic.to", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid value for parameter [map.topic.to]");

        // Trailing "," not allowed
        params.put("map.topic.to", "item1,");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid value for parameter [map.topic.to]");

        params.put("map.topic.to", "aTemplate");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify at least one parameter [field.<...>]");

        params.put("field.", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid parameter [field.<...>]");
        params.remove("field.");

        params.put("field.field1", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid value for parameter [field.field1]");

        params.put("field.field1", "#{}");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Found the invalid expression [#{}] while evaluating [field1]: <Invalid expression>");

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
                        "250");
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
    public void shouldRetrieveLightstreamreClientIdWhenConnectedToConfluentClod(String hostList) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostList);
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        Properties baseConsumerProps = config.baseConsumerProps();
        assertThat(baseConsumerProps)
                .containsAtLeast(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        hostList,
                        ConsumerConfig.CLIENT_ID_CONFIG,
                        ConnectorConfig.LIGHSTREAMER_CLIENT_ID);
    }

    static Stream<String> partialConfluentCloudHostList() {
        return Stream.of(
                "def-437seq1.mycloudrovider2.my.com:9092,lopc-32wwg15.mycloudrovider2.confluent.cloud1:9092");
    }

    @ParameterizedTest
    @MethodSource("partialConfluentCloudHostList")
    public void shouldNonRetrieveLightstreamreClientIdWhenNotAllHostConnectedToConfluentClod(
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
        assertThat(config.getBoolean(ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG)).isFalse();
    }

    @Test
    public void shouldGetText() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.getMetadataAdapterName()).isEqualTo("KAFKA");
        assertThat(config.getAdapterName()).isEqualTo("CONNECTOR");
        assertThat(config.getItemInfoName()).isEqualTo("INFO_ITEM");
        assertThat(config.getItemInfoField()).isEqualTo("INFO_FIELD");

        String groupId = config.getText(ConnectorConfig.GROUP_ID);
        assertThat(groupId).startsWith("KAFKA-CONNECTOR-");
        assertThat(groupId.length()).isGreaterThan("KAFKA-CONNECTOR-".length());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "AVRO",
                "STRING",
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
    public void shouldGetEvaluator(String type) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, type);
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, type);
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getValueEvaluator()).isEqualTo(EvaluatorType.valueOf(type));
        assertThat(config.getKeyEvaluator()).isEqualTo(EvaluatorType.valueOf(type));
    }

    @Test
    public void shouldFailDueToInvalidEvaluatorType() {
        Map<String, String> keys =
                Map.of(
                        ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE,
                        "[record.key.evaluator.type]",
                        ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE,
                        "[record.value.evaluator.type]");
        for (Map.Entry<String, String> entry : keys.entrySet()) {
            Map<String, String> updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.put(entry.getKey(), "invalidType");
            ConfigException e =
                    assertThrows(
                            ConfigException.class,
                            () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
            assertThat(e.getMessage())
                    .isEqualTo("Specify a valid value for parameter " + entry.getValue());
        }
    }

    @Test
    public void shouldFailDueToInvalidSchemaPath() {
        Map<String, String> keys =
                Map.of(
                        ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "[key.evaluator.schema.path]",
                        ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "[value.evaluator.schema.path]");
        for (Map.Entry<String, String> entry : keys.entrySet()) {
            Map<String, String> updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.put(entry.getKey(), "invalidSchemaPath");
            ConfigException e =
                    assertThrows(
                            ConfigException.class,
                            () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
            assertThat(e.getMessage())
                    .isEqualTo(
                            "Not found file [%s/invalidSchemaPath] specified in [%s]"
                                    .formatted(adapterDir, entry.getKey()));
        }
    }

    @Test
    public void shouldFailDueToMissingSchemaPathForAvro() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "AVRO");

        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value either for [record.key.evaluator.schema.path] or [record.key.evaluator.schema.registry.enable]");

        Map<String, String> configs2 = new HashMap<>();
        configs2.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "AVRO");

        ce =
                assertThrows(
                        ConfigException.class, () -> ConnectorConfigProvider.minimalWith(configs2));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value either for [record.value.evaluator.schema.path] or [record.value.evaluator.schema.registry.enable]");
    }

    @Test
    public void shouldGetOverridenGroupId() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.GROUP_ID, "group-id");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getText(ConnectorConfig.GROUP_ID)).isEqualTo("group-id");
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
        assertThat(templateConfig.expressions()).isEmpty();

        ConnectorConfig cgg2 =
                ConnectorConfigProvider.minimalWith(
                        Map.of(
                                "item-template.template1",
                                "item1-#{param1=VALUE.value1}",
                                "item-template.template2",
                                "item2-#{param2=VALUE.value2}"));

        var templateConfigs = cgg2.getItemTemplateConfigs();
        assertThat(templateConfigs.expressions()).hasSize(2);

        TemplateExpression te1 = templateConfigs.getExpression("template1");
        assertThat(te1.prefix()).isEqualTo("item1");
        assertThat(te1.params()).containsExactly("param1", Expressions.expression("VALUE.value1"));

        TemplateExpression te2 = templateConfigs.getExpression("template2");
        assertThat(te2.prefix()).isEqualTo("item2");
        assertThat(te2.params()).containsExactly("param2", Expressions.expression("VALUE.value2"));
    }

    @Test
    void shouldGetFieldConfigs() {
        ConnectorConfig cgg = ConnectorConfigProvider.minimal();
        FieldConfigs fieldConfigs = cgg.getFieldConfigs();
        assertThat(fieldConfigs.expressions()).hasSize(1);
        assertThat(fieldConfigs.getExression("fieldName1").toString()).isEqualTo("VALUE");
    }

    @Test
    public void shouldGetHostList() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getHostsList(ConnectorConfig.BOOTSTRAP_SERVERS))
                .isEqualTo("server:8080,server:8081");
    }

    @Test
    public void shouldGetDefaultText() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getText(ConnectorConfig.ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getText(ConnectorConfig.DATA_ADAPTER_NAME)).isEqualTo("CONNECTOR");
        assertThat(config.getText(ConnectorConfig.ITEM_INFO_NAME)).isEqualTo("INFO");
        assertThat(config.getText(ConnectorConfig.ITEM_INFO_FIELD)).isEqualTo("MSG");
    }

    @Test
    public void shouldGetEnabled() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.isEnabled()).isTrue();

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENABLE, "false");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isEnabled()).isFalse();
    }

    @Test
    public void shouldOverrideConsumeEventsFrom() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_CONSUME_FROM, "EARLIEST");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordConsumeFrom()).isEqualTo(RecordComsumeFrom.EARLIEST);
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
        assertThat(config.getKeyEvaluator()).isEqualTo(EvaluatorType.STRING);
        assertThat(config.getValueEvaluator()).isEqualTo(EvaluatorType.STRING);

        assertThat(config.isSchemaRegistryEnabledForKey()).isFalse();
        assertThat(config.isSchemaRegistryEnabledForKey()).isFalse();
        assertThat(config.isSchemaRegistryEnabled()).isFalse();
    }

    @Test
    public void shouldGetErrorStrategy() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getRecordExtractionErrorHandlingStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE);

        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(
                ConnectorConfig.RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY,
                RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION.toString());
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getRecordExtractionErrorHandlingStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.FORCE_UNSUBSCRIPTION);
    }

    @Test
    public void shouldFailDueToInvalidErrorStragetyType() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, "invalidType");
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter ["
                                + ConnectorConfig.RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY
                                + "]");
    }

    @Test
    public void shouldGetDirectory() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getDirectory(ConnectorConfig.ADAPTER_DIR))
                .isEqualTo(adapterDir.toString());
    }

    @Test
    public void shouldGetFiles() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.getFile(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(keySchemaFile.toString());
        assertThat(config.getFile(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(valueSchemaFile.toString());
    }

    @Test
    public void shouldGetNotExistingNonRequiredFiles() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getFile(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH)).isNull();
        assertThat(config.getFile(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH)).isNull();
    }

    @Test
    public void shouldNoGetNonExistingNonRequiredInt() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_FETCH_MAX_BYTES_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_FETCH_MIN_BYTES_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG))
                .isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_HEARTBEAT_INTERVAL_MS)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_MAX_POLL_RECORDS)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_SESSION_TIMEOUT_MS)).isNull();
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
            assertThat(ce.getMessage())
                    .isEqualTo("Encryption is not enabled. Check parameter [encryption.enable]");
        }
    }

    @Test
    public void shouldSpecifyEncryptionParametersWhenRequired() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENCYRPTION_ENABLE, "true");

        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PATH, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [encryption.truststore.path]");

        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
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
            assertThat(ce.getMessage())
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
        assertThat(ce.getMessage())
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
    public void shoudSpecifyRequiredKeystoreParameters() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.put(EncryptionConfigs.ENABLE_MTLS, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [encryption.keystore.path]");

        updatedConfig.put(EncryptionConfigs.KEYSTORE_PATH, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [encryption.keystore.path]");

        updatedConfig.put(EncryptionConfigs.KEYSTORE_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
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
        updatedConfig.putAll(kesytoreParameters());

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
        updatedConfig.putAll(kesytoreParameters());
        updatedConfig.put(EncryptionConfigs.KEYSTORE_TYPE, "PKCS12");
        updatedConfig.put(EncryptionConfigs.KEYSTORE_PASSWORD, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
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
        assertThat(ce.getMessage())
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
    public void shouldSpecifyAuthenticationRequiredParameters() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");

        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "invalid");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [authentication.mechanism]");
        // Restore default SASL/PLAIN mechanism
        updatedConfig.remove(BrokerAuthenticationConfigs.SASL_MECHANISM);

        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.username]");

        updatedConfig.put(BrokerAuthenticationConfigs.USERNAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [authentication.username]");

        updatedConfig.put(BrokerAuthenticationConfigs.USERNAME, "username");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.password]");

        updatedConfig.put(BrokerAuthenticationConfigs.PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [authentication.password]");

        updatedConfig.put(BrokerAuthenticationConfigs.PASSWORD, "password");
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
                        () -> config.isGssapiEnabled(),
                        () -> config.gssapiKerberosServiceName(),
                        () -> config.gssapiKeyTab(),
                        () -> config.gssapiStoreKey(),
                        () -> config.gssapiPrincipal(),
                        () -> config.gssapiUseKeyTab(),
                        () -> config.gssapiUseTicketCache());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce.getMessage())
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
    public void shouldSpecifyGssapiAuthenticationRequiredParameters() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [authentication.gssapi.kerberos.service.name]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [authentication.gssapi.kerberos.service.name]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.gssapi.principal]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [authentication.gssapi.principal]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldGetDefaultGssapiAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");
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
        updatedConfig.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");
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
        updatedConfig.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");
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
        updatedConfig.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.gssapi.key.tab.path]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
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
        updatedConfig.put(ConnectorConfig.AUTHENTICATION_ENABLE, "true");
        updatedConfig.put(BrokerAuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.gssapi.key.tab.path]");

        updatedConfig.put(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
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
            assertThat(ce.getMessage())
                    .isEqualTo(
                            "Neither parameter [record.key.evaluator.schema.registry.enable] nor parameter [record.value.evaluator.schema.registry.enable] are enabled");
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE
            })
    public void shouldSpecifyRequiredSchemaRegistryParameters(String evaluatorKey) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(evaluatorKey, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage()).isEqualTo("Missing required parameter [schema.registry.url]");

        updatedConfig.put(SchemaRegistryConfigs.URL, "http://localhost:8080");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldNotAccessToSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.URL, "http://localhost:8080");

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
            assertThat(ce.getMessage())
                    .isEqualTo("Parameter [schema.registry.url] is not set with https protocol");
        }
    }

    @Test
    public void shouldGetDefaultSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.URL, "https://localhost:8080");

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
            assertThat(ce.getMessage())
                    .isEqualTo(
                            "Parameter [schema.registry.encryption.keystore.enable] is not enabled");
        }
    }

    @Test
    public void shouldOverrideSchemaRegistryEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.URL, "https://localhost:8080");
        updatedConfig.put(
                SchemaRegistryConfigs.TRUSTSTORE_PATH, trustStoreFile.getFileName().toString());
        updatedConfig.put(SchemaRegistryConfigs.TRUSTSTORE_PASSWORD, "truststore-password");
        updatedConfig.put(SchemaRegistryConfigs.SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        updatedConfig.put(SchemaRegistryConfigs.SSL_PROTOCOL, "TLSv1.2");
        updatedConfig.put(SchemaRegistryConfigs.TRUSTSTORE_TYPE, "PKCS12");
        updatedConfig.put(
                SchemaRegistryConfigs.SSL_CIPHER_SUITES,
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        updatedConfig.put(SchemaRegistryConfigs.HOSTNAME_VERIFICATION_ENANLE, "true");

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
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.URL, "https://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.KEYSTORE_ENABLE, "true");
        updatedConfig.put(
                SchemaRegistryConfigs.KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        updatedConfig.put(SchemaRegistryConfigs.KEYSTORE_PASSWORD, "keystore-password");

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
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.URL, "https://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.KEYSTORE_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.KEYSTORE_TYPE, "PKCS12");
        updatedConfig.put(
                SchemaRegistryConfigs.KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        updatedConfig.put(SchemaRegistryConfigs.KEYSTORE_PASSWORD, "keystore-password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isSchemaRegistryKeystoreEnabled()).isTrue();
        assertThat(config.schemaRegistryKeystoreType().toString()).isEqualTo("PKCS12");

        updatedConfig.put(SchemaRegistryConfigs.KEY_PASSWORD, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.encryption.keystore.key.password]");

        updatedConfig.put(SchemaRegistryConfigs.KEY_PASSWORD, "key-password");
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
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.URL, "http://localhost:8080");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryBasicAuthenticationEnabled()).isFalse();

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.schemaRegistryBasicAuthenticationUserName(),
                        () -> config.schemaRegistryBasicAuthenticationPassword());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce.getMessage())
                    .isEqualTo(
                            "Parameter [schema.registry.basic.authentication.enabled] is not enabled");
        }
    }

    @Test
    public void shouldGetSchemaRegistryBasicAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfig.put(SchemaRegistryConfigs.URL, "http://localhost:8080");
        updatedConfig.put(SchemaRegistryConfigs.ENABLE_BASIC_AUTHENTICATION, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [schema.registry.basic.authentication.username]");

        updatedConfig.put(SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_NAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.basic.authentication.username]");

        updatedConfig.put(SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_NAME, "username");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [schema.registry.basic.authentication.password]");

        updatedConfig.put(SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [schema.registry.basic.authentication.password]");

        updatedConfig.put(SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_PASSWORD, "password");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isSchemaRegistryBasicAuthenticationEnabled()).isTrue();
        assertThat(config.schemaRegistryBasicAuthenticationUserName()).isEqualTo("username");
        assertThat(config.schemaRegistryBasicAuthenticationPassword()).isEqualTo("password");

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
                        "USER_INFO",
                        SchemaRegistryClientConfig.USER_INFO_CONFIG,
                        "username:password");
    }
}
