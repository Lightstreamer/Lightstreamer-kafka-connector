
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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SslProtocol.TLSv12;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SslProtocol.TLSv13;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        ConfigSpec configSpec = ConnectorConfig.configSpec();

        ConfParameter adapterDir = configSpec.getParameter(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDir.name()).isEqualTo(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDir.required()).isTrue();
        assertThat(adapterDir.multiple()).isFalse();
        assertThat(adapterDir.mutable()).isTrue();
        assertThat(adapterDir.defaultValue()).isNull();
        assertThat(adapterDir.type()).isEqualTo(ConfType.DIRECTORY);

        ConfParameter enabled = configSpec.getParameter(ConnectorConfig.ENABLED);
        assertThat(enabled.name()).isEqualTo(ConnectorConfig.ENABLED);
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
                configSpec.getParameter(ConnectorConfig.KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.name()).isEqualTo(ConnectorConfig.KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.required()).isFalse();
        assertThat(keyEvaluatorType.multiple()).isFalse();
        assertThat(keyEvaluatorType.mutable()).isTrue();
        assertThat(keyEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(keyEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter keySchemaFile =
                configSpec.getParameter(ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaFile.name()).isEqualTo(ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH);
        assertThat(keySchemaFile.required()).isFalse();
        assertThat(keySchemaFile.multiple()).isFalse();
        assertThat(keySchemaFile.mutable()).isTrue();
        assertThat(keySchemaFile.defaultValue()).isNull();
        assertThat(keySchemaFile.type()).isEqualTo(ConfType.FILE);

        ConfParameter valueEvaluatorType =
                configSpec.getParameter(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.name()).isEqualTo(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.required()).isFalse();
        assertThat(valueEvaluatorType.multiple()).isFalse();
        assertThat(valueEvaluatorType.mutable()).isTrue();
        assertThat(valueEvaluatorType.defaultValue()).isEqualTo("STRING");
        assertThat(valueEvaluatorType.type()).isEqualTo(ConfType.EVALUATOR);

        ConfParameter valueSchemaFile =
                configSpec.getParameter(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaFile.name()).isEqualTo(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH);
        assertThat(valueSchemaFile.required()).isFalse();
        assertThat(valueSchemaFile.multiple()).isFalse();
        assertThat(valueSchemaFile.mutable()).isTrue();
        assertThat(valueSchemaFile.defaultValue()).isNull();
        assertThat(valueSchemaFile.type()).isEqualTo(ConfType.FILE);

        ConfParameter itemTemplate = configSpec.getParameter(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.name()).isEqualTo(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.required()).isTrue();
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
        assertThat(topicMapping.type()).isEqualTo(ConfType.TEXT);

        ConfParameter fieldMapping = configSpec.getParameter(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.name()).isEqualTo(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.required()).isTrue();
        assertThat(fieldMapping.multiple()).isTrue();
        assertThat(fieldMapping.suffix()).isNull();
        assertThat(fieldMapping.mutable()).isTrue();
        assertThat(fieldMapping.defaultValue()).isNull();
        assertThat(fieldMapping.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyEvaluatorSchemaRegistryUrl =
                configSpec.getParameter(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(keyEvaluatorSchemaRegistryUrl.name())
                .isEqualTo(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(keyEvaluatorSchemaRegistryUrl.required()).isFalse();
        assertThat(keyEvaluatorSchemaRegistryUrl.multiple()).isFalse();
        assertThat(keyEvaluatorSchemaRegistryUrl.mutable()).isTrue();
        assertThat(keyEvaluatorSchemaRegistryUrl.defaultValue()).isNull();
        assertThat(keyEvaluatorSchemaRegistryUrl.type()).isEqualTo(ConfType.URL);

        ConfParameter valueEvaluatorSchemaRegistryUrl =
                configSpec.getParameter(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(valueEvaluatorSchemaRegistryUrl.name())
                .isEqualTo(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(valueEvaluatorSchemaRegistryUrl.required()).isFalse();
        assertThat(valueEvaluatorSchemaRegistryUrl.multiple()).isFalse();
        assertThat(valueEvaluatorSchemaRegistryUrl.mutable()).isTrue();
        assertThat(valueEvaluatorSchemaRegistryUrl.defaultValue()).isNull();
        assertThat(valueEvaluatorSchemaRegistryUrl.type()).isEqualTo(ConfType.URL);

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

        ConfParameter autoOffsetReset =
                configSpec.getParameter(ConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        assertThat(autoOffsetReset.name())
                .isEqualTo(ConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        assertThat(autoOffsetReset.required()).isFalse();
        assertThat(autoOffsetReset.multiple()).isFalse();
        assertThat(autoOffsetReset.mutable()).isTrue();
        assertThat(autoOffsetReset.defaultValue()).isEqualTo("latest");
        assertThat(autoOffsetReset.type()).isEqualTo(ConfType.TEXT);

        ConfParameter encryptionEnabed =
                configSpec.getParameter(ConnectorConfig.ENABLE_ENCRYTPTION);
        assertThat(encryptionEnabed.name()).isEqualTo(ConnectorConfig.ENABLE_ENCRYTPTION);
        assertThat(encryptionEnabed.required()).isFalse();
        assertThat(encryptionEnabed.multiple()).isFalse();
        assertThat(encryptionEnabed.mutable()).isTrue();
        assertThat(encryptionEnabed.defaultValue()).isEqualTo("false");
        assertThat(encryptionEnabed.type()).isEqualTo(ConfType.BOOL);

        ConfParameter authenticationEnabled =
                configSpec.getParameter(ConnectorConfig.ENABLE_AUTHENTICATION);
        assertThat(authenticationEnabled.name()).isEqualTo(ConnectorConfig.ENABLE_AUTHENTICATION);
        assertThat(authenticationEnabled.required()).isFalse();
        assertThat(authenticationEnabled.multiple()).isFalse();
        assertThat(authenticationEnabled.mutable()).isTrue();
        assertThat(authenticationEnabled.defaultValue()).isEqualTo("false");
        assertThat(authenticationEnabled.type()).isEqualTo(ConfType.BOOL);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
						KEY                      | EXPECTED_INFIX
						map.topic.to             | topic
						map.topicprefix.topic.to | topicprefix.topic
						map.topic                | ''
						pam.topic.to             | ''
						map.map.my.topic.to.to   | map.my.topic.to
						""")
    public void shouldExtractInfixForMap(String key, String expectedInfix) {
        ConfigSpec configSpec = ConnectorConfig.configSpec();
        Optional<String> infix =
                ConfigSpec.extractInfix(
                        configSpec.getParameter(ConnectorConfig.TOPIC_MAPPING), key);
        if (!expectedInfix.isBlank()) {
            assertThat(infix).hasValue(expectedInfix);
        } else {
            assertThat(infix).isEmpty();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
						KEY                      | EXPECTED_INFIX
						field.name               | name
						myfield.name             | ''
						field.my.name            | my.name
						""")
    public void shouldGetInfixForField(String key, String expectedInfix) {
        ConfigSpec configSpec = ConnectorConfig.configSpec();
        Optional<String> infix =
                ConfigSpec.extractInfix(
                        configSpec.getParameter(ConnectorConfig.FIELD_MAPPING), key);
        if (!expectedInfix.isBlank()) {
            assertThat(infix).hasValue(expectedInfix);
        } else {
            assertThat(infix).isEmpty();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
						KEY                        | EXPECTED_INFIX
						item-template.template1    | template1
						myitem.template1           | ''
						item-template.my.template1 | my.template1
						""")
    public void shouldGetInfixForItemTemplate(String key, String expectedInfix) {
        ConfigSpec configSpec = ConnectorConfig.configSpec();
        Optional<String> infix =
                ConfigSpec.extractInfix(
                        configSpec.getParameter(ConnectorConfig.ITEM_TEMPLATE), key);
        if (!expectedInfix.isBlank()) {
            assertThat(infix).hasValue(expectedInfix);
        } else {
            assertThat(infix).isEmpty();
        }
    }

    private Map<String, String> standardParameters() {
        Map<String, String> standardParams = new HashMap<>();
        standardParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        standardParams.put(ConnectorConfig.VALUE_EVALUATOR_TYPE, "STRING");
        standardParams.put(
                ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH,
                valueSchemaFile.getFileName().toString());
        standardParams.put(
                ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL,
                "http://value-host:8080/registry");
        standardParams.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "JSON");
        standardParams.put(
                ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH, keySchemaFile.getFileName().toString());
        standardParams.put(
                ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, "http://key-host:8080/registry");
        standardParams.put(ConnectorConfig.ITEM_INFO_NAME, "INFO_ITEM");
        standardParams.put(ConnectorConfig.ITEM_INFO_FIELD, "INFO_FIELD");
        standardParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        standardParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
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
        standardParams.put("item-template.template1", "item1");
        standardParams.put("item-template.template2", "item2");
        standardParams.put("map.topic1.to", "template1");
        standardParams.put("map.topic2.to", "template2");
        standardParams.put("field.fieldName1", "bar");
        return standardParams;
    }

    private Map<String, String> encryptionParameters() {
        Map<String, String> encryptionParams = new HashMap<>();
        encryptionParams.put(ConnectorConfig.ENABLE_ENCRYTPTION, "true");
        // encryptionParams.put(EncryptionConfigs.TRUSTSTORE_PATH, trustStoreFile.toString());
        // encryptionParams.put(EncryptionConfigs.TRUSTSTORE_PASSWORD, "truststore-password");
        return encryptionParams;
    }

    private Map<String, String> kesytoreParameters() {
        Map<String, String> keystoreParams = new HashMap<>();
        keystoreParams.put(EncryptionConfigs.ENABLE_MTLS, "true");
        keystoreParams.put(KeystoreConfigs.KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        keystoreParams.put(KeystoreConfigs.KEYSTORE_PASSWORD, "keystore-password");
        return keystoreParams;
    }

    private Map<String, String> authenticationParameters() {
        Map<String, String> authParams = new HashMap<>();
        authParams.put(ConnectorConfig.ENABLE_AUTHENTICATION, "true");
        authParams.put(AuthenticationConfigs.USERNAME, "sasl-username");
        authParams.put(AuthenticationConfigs.PASSWORD, "sasl-password");
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
                                .formatted(ConnectorConfig.BOOTSTRAP_SERVERS));

        params.put(ConnectorConfig.BOOTSTRAP_SERVERS, "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(ConnectorConfig.BOOTSTRAP_SERVERS));

        params.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080");
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
                .isEqualTo("Specify at least one parameter [item-template.<...>]");

        params.put("item-template.template1", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [item-template.template1]");

        params.put("item-template.template1", "template");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify at least one parameter [map.<...>.to]");

        params.put("map.topic.to", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid value for parameter [map.topic.to]");

        params.put("map.topic.to", "aTemplate");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify at least one parameter [field.<...>]");

        params.put("field.field1", "");
        ce = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid value for parameter [field.field1]");

        params.put("field.field1", "VALUE");
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
    @ValueSource(strings = {"AVRO", "STRING", "JSON"})
    public void shouldGetEvaluator(String type) {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.KEY_EVALUATOR_TYPE, type);
        updatedConfig.put(ConnectorConfig.VALUE_EVALUATOR_TYPE, type);
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getEvaluator(ConnectorConfig.VALUE_EVALUATOR_TYPE))
                .isEqualTo(EvaluatorType.valueOf(type));
        assertThat(config.getEvaluator(ConnectorConfig.KEY_EVALUATOR_TYPE))
                .isEqualTo(EvaluatorType.valueOf(type));
    }

    @Test
    public void shouldFailDueToInvaludEvaluatorType() {
        Map<String, String> keys =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_TYPE,
                        "[key.evaluator.type]",
                        ConnectorConfig.VALUE_EVALUATOR_TYPE,
                        "[value.evaluator.type]");
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
    public void shouldFailDueToInvaludSchemaPath() {
        Map<String, String> keys =
                Map.of(
                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH,
                        "[key.evaluator.schema.path]",
                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH,
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
    public void shouldGetOverridenGroupId() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.GROUP_ID, "group-id");
        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.getText(ConnectorConfig.GROUP_ID)).isEqualTo("group-id");
    }

    @Test
    public void shouldGetValues() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        Map<String, String> topics = config.getValues(ConnectorConfig.TOPIC_MAPPING, true);
        assertThat(topics).containsExactly("topic1", "template1", "topic2", "template2");

        Map<String, String> itemTemplates = config.getValues(ConnectorConfig.ITEM_TEMPLATE, true);
        assertThat(itemTemplates).containsExactly("template1", "item1", "template2", "item2");

        Map<String, String> noRemappledItemTemplates =
                config.getValues(ConnectorConfig.ITEM_TEMPLATE, false);
        assertThat(noRemappledItemTemplates)
                .containsExactly(
                        "item-template.template1", "item1", "item-template.template2", "item2");
    }

    @Test
    public void shouldGetAsList() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        List<String> values =
                config.getAsList(
                        ConnectorConfig.TOPIC_MAPPING, e -> e.getKey() + "_" + e.getValue());
        assertThat(values).containsExactly("topic1_template1", "topic2_template2");
    }

    @Test
    public void shouldGetItemTemplateList() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        List<String> values =
                config.getAsList(
                        ConnectorConfig.ITEM_TEMPLATE, e -> e.getKey() + "_" + e.getValue());
        assertThat(values).containsExactly("template1_item1", "template2_item2");
    }

    @Test
    public void shouldGetUrl() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false))
                .isEqualTo("http://key-host:8080/registry");
        assertThat(config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false))
                .isEqualTo("http://value-host:8080/registry");
    }

    @Test
    public void shouldGetRequiredUrl() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());
        assertThat(config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, true))
                .isEqualTo("http://key-host:8080/registry");
        assertThat(config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, true))
                .isEqualTo("http://value-host:8080/registry");
    }

    @Test
    public void shouldNotGetRequiredHost() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();

        String keySchemaRegistryUrl =
                config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false);
        assertThat(keySchemaRegistryUrl).isNull();

        String valuechemaRegistryUrl =
                config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false);
        assertThat(valuechemaRegistryUrl).isNull();

        ConfigException exception =
                assertThrows(
                        ConfigException.class,
                        () ->
                                config.getUrl(
                                        ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, true));
        assertThat(exception.getMessage())
                .isEqualTo("Missing required parameter [key.evaluator.schema.registry.url]");

        ConfigException exception2 =
                assertThrows(
                        ConfigException.class,
                        () ->
                                config.getUrl(
                                        ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, true));
        assertThat(exception2.getMessage())
                .isEqualTo("Missing required parameter [value.evaluator.schema.registry.url]");
    }

    @Test
    public void shouldGetHostLists() {
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
        updatedConfig.put(ConnectorConfig.ENABLED, "false");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.isEnabled()).isFalse();
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
        assertThat(config.getEvaluator(ConnectorConfig.KEY_EVALUATOR_TYPE))
                .isEqualTo(EvaluatorType.STRING);
        assertThat(config.getEvaluator(ConnectorConfig.VALUE_EVALUATOR_TYPE))
                .isEqualTo(EvaluatorType.STRING);
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
    public void shouldNotGetNonExistingNonRequiredHost() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false))
                .isNull();
        assertThat(config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false))
                .isNull();
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
        assertThat(config.getFile(ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(keySchemaFile.toString());
        assertThat(config.getFile(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH))
                .isEqualTo(valueSchemaFile.toString());
    }

    @Test
    public void shouldGetNotExistingNonRequiredFiles() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir.toString());
        assertThat(config.getFile(ConnectorConfig.KEY_EVALUATOR_SCHEMA_PATH)).isNull();
        assertThat(config.getFile(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_PATH)).isNull();
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
        assertThrows(ConfigException.class, () -> config.isKeystoreEnabled());
        assertThrows(ConfigException.class, () -> config.getEnabledProtocols());
        assertThrows(ConfigException.class, () -> config.getEnabledProtocolsAsStr());
        assertThrows(ConfigException.class, () -> config.getSslProtocol());
        assertThrows(ConfigException.class, () -> config.getTrustStoreType());
        assertThrows(ConfigException.class, () -> config.getTrustStorePath());
        assertThrows(ConfigException.class, () -> config.getTrustStorePassword());
        assertThrows(ConfigException.class, () -> config.isHostNameVerificationEnabled());
        assertThrows(ConfigException.class, () -> config.getCipherSuites());
        assertThrows(ConfigException.class, () -> config.getCipherSuitesAsStr());
        assertThrows(ConfigException.class, () -> config.getSslProvider());
    }

    @Test
    public void shouldSpecifyEncryptionParametersWhenRequired() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENABLE_ENCRYTPTION, "true");

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
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [encryption.truststore.password]");

        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [encryption.truststore.password]");
        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PASSWORD, "password");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldGetDefaultEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isEncryptionEnabled()).isTrue();
        assertThat(config.getEnabledProtocols()).containsExactly(TLSv12, TLSv13);
        assertThat(config.getEnabledProtocolsAsStr()).isEqualTo("TLSv1.2,TLSv1.3");
        assertThat(config.getSslProtocol().toString()).isEqualTo("TLSv1.3");
        assertThat(config.getTrustStoreType().toString()).isEqualTo("JKS");
        assertThat(config.isHostNameVerificationEnabled()).isFalse();
        assertThat(config.getCipherSuites()).isEmpty();
        assertThat(config.getCipherSuitesAsStr()).isNull();
        assertThat(config.getSslProvider()).isNull();
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
        assertThat(props).doesNotContainKey(SslConfigs.SSL_CIPHER_SUITES_CONFIG);

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.getKeystorePath(),
                        () -> config.getKeystorePassword(),
                        () -> config.getKeystoreType(),
                        () -> config.getKeyPassword());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce.getMessage())
                    .isEqualTo("Parameter [encryption.keystore.enabled] is not enabled");
        }
    }

    @Test
    public void shouldOverrideEncryptionSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.put(
                EncryptionConfigs.TRUSTSTORE_PATH, trustStoreFile.getFileName().toString());
        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_PASSWORD, "truststore-password");
        updatedConfig.put(EncryptionConfigs.SSL_ENABLED_PROTOCOLS, "TLSv1.2");
        updatedConfig.put(EncryptionConfigs.SSL_PROTOCOL, "TLSv1.2");
        updatedConfig.put(EncryptionConfigs.TRUSTSTORE_TYPE, "PKCS12");
        updatedConfig.put(
                EncryptionConfigs.SSL_CIPHER_SUITES,
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        updatedConfig.put(EncryptionConfigs.ENABLE_HOSTNAME_VERIFICATION, "true");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isEncryptionEnabled()).isTrue();
        assertThat(config.getEnabledProtocols()).containsExactly(TLSv12);
        assertThat(config.getEnabledProtocolsAsStr()).isEqualTo("TLSv1.2");
        assertThat(config.getSslProtocol().toString()).isEqualTo("TLSv1.2");
        assertThat(config.getTrustStoreType().toString()).isEqualTo("PKCS12");
        assertThat(config.getTrustStorePath()).isEqualTo(trustStoreFile.toString());
        assertThat(config.getTrustStorePassword()).isEqualTo("truststore-password");
        assertThat(config.isHostNameVerificationEnabled()).isTrue();
        assertThat(config.getCipherSuites())
                .containsExactly(
                        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.getCipherSuitesAsStr())
                .isEqualTo("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA");
        assertThat(config.getSslProvider()).isNull();

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

        updatedConfig.put(KeystoreConfigs.KEYSTORE_PATH, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [encryption.keystore.path]");

        updatedConfig.put(KeystoreConfigs.KEYSTORE_PATH, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Not found file ["
                                + adapterDir.toString()
                                + "/aFile] specified in [encryption.keystore.path]");

        updatedConfig.put(KeystoreConfigs.KEYSTORE_PATH, keyStoreFile.getFileName().toString());
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [encryption.keystore.password]");

        updatedConfig.put(KeystoreConfigs.KEYSTORE_PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [encryption.keystore.password]");

        updatedConfig.put(KeystoreConfigs.KEYSTORE_PASSWORD, "password");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldGetDefaultKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.putAll(kesytoreParameters());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isKeystoreEnabled()).isTrue();
        assertThat(config.getKeystorePath()).isEqualTo(keyStoreFile.toString());
        assertThat(config.getKeystoreType().toString()).isEqualTo("JKS");
        assertThat(config.getKeystorePassword()).isEqualTo("keystore-password");
        assertThat(config.getKeyPassword()).isNull();

        Properties props = config.baseConsumerProps();
        assertThat(props)
                .containsAtLeast(
                        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                        "JKS",
                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        "keystore-password",
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        keyStoreFile.toString());
        assertThat(props).doesNotContainKey(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    }

    @Test
    public void shouldOverrideKeystoreSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(encryptionParameters());
        updatedConfig.putAll(kesytoreParameters());
        updatedConfig.put(KeystoreConfigs.KEYSTORE_TYPE, "PKCS12");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isKeystoreEnabled()).isTrue();
        assertThat(config.getKeystoreType().toString()).isEqualTo("PKCS12");

        updatedConfig.put(KeystoreConfigs.KEY_PASSWORD, "");
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [encryption.key.password]");

        updatedConfig.put(KeystoreConfigs.KEY_PASSWORD, "key-password");
        config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);
        assertThat(config.getKeyPassword()).isEqualTo("key-password");

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
    public void shouldSpecifyPlainAuthenticationRequiredParameters() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENABLE_AUTHENTICATION, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.username]");

        updatedConfig.put(AuthenticationConfigs.USERNAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [authentication.username]");

        updatedConfig.put(AuthenticationConfigs.USERNAME, "username");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.password]");

        updatedConfig.put(AuthenticationConfigs.PASSWORD, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [authentication.password]");

        updatedConfig.put(AuthenticationConfigs.PASSWORD, "password");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldNotAccessToAuthenticationSettings() {
        ConnectorConfig config =
                ConnectorConfig.newConfig(adapterDir.toFile(), standardParameters());

        assertThat(config.isAuthenticationEnabled()).isFalse();

        List<ThrowingRunnable> runnables =
                List.of(
                        () -> config.getAuthenticationMechanism(),
                        () -> config.getAuthenticationUsername(),
                        () -> config.getAuthenticationPassword(),
                        () -> config.isGssapiEnabled(),
                        () -> config.gssapiKerberosServiceName(),
                        () -> config.gssapiKeyTab(),
                        () -> config.gssapiStoreKey(),
                        () -> config.gssapiPrincipal(),
                        () -> config.gssapiUseKeyTab());
        for (ThrowingRunnable executable : runnables) {
            ConfigException ce = assertThrows(ConfigException.class, executable);
            assertThat(ce.getMessage())
                    .isEqualTo("Parameter [authentication.enabled] is not enabled");
        }
    }

    @Test
    public void shouldGetDefaultAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.putAll(authenticationParameters());

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isAuthenticationEnabled()).isTrue();
        assertThat(config.getAuthenticationMechanism().toString()).isEqualTo("PLAIN");

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
        List<String> mechanisms =
                Stream.of(SaslMechanism.SCRAM_256, SaslMechanism.SCRAM_512)
                        .map(Object::toString)
                        .toList();

        for (boolean encrypted : List.of(true, false)) {
            Map<String, String> updatedConfig = new HashMap<>(standardParameters());
            updatedConfig.putAll(authenticationParameters());
            // Test both encrypted and clear channels
            if (encrypted) {
                updatedConfig.putAll(encryptionParameters());
            }
            for (String mechanism : mechanisms) {
                updatedConfig.put(AuthenticationConfigs.SASL_MECHANISM, mechanism);
                ConnectorConfig config =
                        ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

                assertThat(config.isAuthenticationEnabled()).isTrue();
                assertThat(config.getAuthenticationMechanism().toString()).isEqualTo(mechanism);
                assertThat(config.getAuthenticationUsername()).isEqualTo("sasl-username");
                assertThat(config.getAuthenticationPassword()).isEqualTo("sasl-password");

                Properties properties = config.baseConsumerProps();
                assertThat(properties)
                        .containsAtLeast(
                                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                encrypted
                                        ? SecurityProtocol.SASL_SSL.toString()
                                        : SecurityProtocol.SASL_PLAINTEXT.toString(),
                                SaslConfigs.SASL_MECHANISM,
                                mechanism,
                                SaslConfigs.SASL_JAAS_CONFIG,
                                "org.apache.kafka.common.security.scram.ScramLoginModule required username='sasl-username' password='sasl-password';");
            }
        }
    }

    @Test
    public void shouldGetDefaultGssapiAuthenticationRequiredParameters() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENABLE_AUTHENTICATION, "true");
        updatedConfig.put(AuthenticationConfigs.SASL_MECHANISM, "GSSAPI");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.gssapi.principal]");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_PRINCIPAL, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [authentication.gssapi.principal]");

        updatedConfig.put(AuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Missing required parameter [authentication.gssapi.kerberos.service.name]");

        updatedConfig.put(AuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [authentication.gssapi.kerberos.service.name]");

        updatedConfig.put(AuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }

    @Test
    public void shouldGetDefaultGssapiAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENABLE_AUTHENTICATION, "true");
        updatedConfig.put(AuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");

        ConnectorConfig config = ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig);

        assertThat(config.isGssapiEnabled()).isTrue();
        assertThat(config.gssapiUseKeyTab()).isFalse();
        assertThat(config.gssapiKeyTab()).isNull();
        assertThat(config.gssapiStoreKey()).isFalse();
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
                        "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=false storeKey=false principal='kafka-user';");
    }

    @Test
    public void shouldOverrideGssapiAuthenticationSettings() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENABLE_AUTHENTICATION, "true");
        updatedConfig.put(AuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_STORE_KEY, "true");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_USE_KEY_TAB, "true");
        updatedConfig.put(
                AuthenticationConfigs.GSSAPI_KEY_TAB, keyTabFile.getFileName().toString());

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
    public void shouldNotValidateWhenKeyTabIsNotSpecified() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.ENABLE_AUTHENTICATION, "true");
        updatedConfig.put(AuthenticationConfigs.SASL_MECHANISM, "GSSAPI");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_PRINCIPAL, "kafka-user");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME, "kafka");
        updatedConfig.put(AuthenticationConfigs.GSSAPI_USE_KEY_TAB, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo("Missing required parameter [authentication.gssapi.key.tab]");

        updatedConfig.put(AuthenticationConfigs.GSSAPI_KEY_TAB, "aFile");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Not found file ["
                                + adapterDir.toString()
                                + "/aFile] specified in [authentication.gssapi.key.tab]");

        updatedConfig.put(
                AuthenticationConfigs.GSSAPI_KEY_TAB, keyTabFile.getFileName().toString());
        assertDoesNotThrow(() -> ConnectorConfig.newConfig(adapterDir.toFile(), updatedConfig));
    }
}
