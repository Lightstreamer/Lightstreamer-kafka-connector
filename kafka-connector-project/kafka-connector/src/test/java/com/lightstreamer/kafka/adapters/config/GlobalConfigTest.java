
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.STRING;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.DestinationMappingConfig;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
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
import java.util.function.Function;
import java.util.stream.Stream;

class GlobalConfigTest {

    private Path loggingConfigurationFile;
    private Path adapterDir;

    @BeforeEach
    void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
        loggingConfigurationFile = Files.createTempFile(adapterDir, "log4j-", ".properties");
    }

    @AfterEach
    void after() throws IOException {
        Files.delete(loggingConfigurationFile);
        FileUtils.deleteDirectory(adapterDir.toFile());
    }

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = GlobalConfig.configSpec();

        ConfParameter adaptersConfId = configSpec.findParameter(GlobalConfig.ADAPTERS_CONF_ID);
        assertThat(adaptersConfId.name()).isEqualTo(GlobalConfig.ADAPTERS_CONF_ID);
        assertThat(adaptersConfId.required()).isTrue();
        assertThat(adaptersConfId.multiple()).isFalse();
        assertThat(adaptersConfId.mutable()).isTrue();
        assertThat(adaptersConfId.defaultValue()).isNull();
        assertThat(adaptersConfId.type()).isEqualTo(TEXT);

        ConfParameter loggingConfigurationPath =
                configSpec.findParameter(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        assertThat(loggingConfigurationPath.name())
                .isEqualTo(GlobalConfig.LOGGING_CONFIGURATION_PATH);
        assertThat(loggingConfigurationPath.required()).isTrue();
        assertThat(loggingConfigurationPath.multiple()).isFalse();
        assertThat(loggingConfigurationPath.mutable()).isTrue();
        assertThat(loggingConfigurationPath.defaultValue()).isNull();
        assertThat(loggingConfigurationPath.type()).isEqualTo(FILE);
    }

    @Test
    void shouldSpecifyRequiredParam() {
        ConfigException e =
                assertThrows(ConfigException.class, () -> new GlobalConfig(Collections.emptyMap()));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [%s]".formatted(GlobalConfig.ADAPTERS_CONF_ID));

        Map<String, String> params = new HashMap<>();
        params.put(GlobalConfig.ADAPTERS_CONF_ID, "");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.ADAPTERS_CONF_ID));

        params.put(GlobalConfig.ADAPTERS_CONF_ID, "KAFKA");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(GlobalConfig.LOGGING_CONFIGURATION_PATH));

        params.put(GlobalConfig.LOGGING_CONFIGURATION_PATH, "non-existing-conf-file");
        e = assertThrows(ConfigException.class, () -> new GlobalConfig(params));
        assertThat(e)
                .hasMessageThat()
                .isEqualTo(
                        "Not found file [non-existing-conf-file] specified in [%s]"
                                .formatted(GlobalConfig.LOGGING_CONFIGURATION_PATH));

        params.put(
                GlobalConfig.LOGGING_CONFIGURATION_PATH,
                Path.of(adapterDir.toString(), loggingConfigurationFile.getFileName().toString())
                        .toString());
        assertDoesNotThrow(() -> new GlobalConfig(params));
    }

    private Map<String, String> minimal() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");

        // Ensure we are specifying a path name relative to the provided adapter dir.
        String farthestPathName = loggingConfigurationFile.getFileName().toString();
        adapterParams.put(GlobalConfig.LOGGING_CONFIGURATION_PATH, farthestPathName);

        return adapterParams;
    }

    private Map<String, String> producerEnabled() {
        Map<String, String> params = minimal();
        params.put(GlobalConfig.PRODUCER_ENABLE, "true");
        params.put(GlobalConfig.PRODUCER_BOOTSTRAP_SERVERS, "localhost:9092");
        params.put("producer.map.destination.to", "test-topic");
        return params;
    }

    @Test
    void shouldGetNewConfig() {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), minimal());
        assertThat(config.getText(GlobalConfig.ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getFile(GlobalConfig.LOGGING_CONFIGURATION_PATH))
                .isEqualTo(loggingConfigurationFile.toString());
    }

    static Stream<Named<Function<GlobalConfig, Object>>> producerSettingsAccessors() {
        return Stream.of(
                Named.of("getProducerKeyEvaluatorType", GlobalConfig::getProducerKeyEvaluatorType),
                Named.of(
                        "getProducerValueEvaluatorType",
                        GlobalConfig::getProducerValueEvaluatorType),
                Named.of(
                        "getProducerSendConfirmationMode",
                        GlobalConfig::getProducerSendConfirmationMode),
                Named.of("baseProducerProps", GlobalConfig::baseProducerProps));
    }

    @ParameterizedTest
    @MethodSource("producerSettingsAccessors")
    void shouldNotAccessProducerSettings(Function<GlobalConfig, Object> accessor) {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), minimal());
        assertThat(config.isProducerEnabled()).isFalse();

        ConfigException ce = assertThrows(ConfigException.class, () -> accessor.apply(config));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Producer is not enabled. Check parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_ENABLE));
    }

    @Test
    void shouldSpecifyProducerParametersWhenRequired() {
        Map<String, String> params = minimal();
        params.put(GlobalConfig.PRODUCER_ENABLE, "true");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_BOOTSTRAP_SERVERS));

        params.put(GlobalConfig.PRODUCER_BOOTSTRAP_SERVERS, "");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_BOOTSTRAP_SERVERS));

        params.put(GlobalConfig.PRODUCER_BOOTSTRAP_SERVERS, "localhost:9092");
        ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify at least one parameter [%s.<...>.%s]"
                                .formatted(
                                        GlobalConfig.PRODUCER_DESTINATION_MAPPING,
                                        GlobalConfig.PRODUCER_MAP_TO_TOPICS_SUFFIX));

        params.put("producer.map.destination.to", "test-topic");
        assertDoesNotThrow(() -> GlobalConfig.newConfig(adapterDir.toFile(), params));
    }

    @Test
    void shouldGetDefaultRecordEvaluatorType() {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), producerEnabled());
        assertThat(config.getProducerKeyEvaluatorType()).isEqualTo(STRING);
        assertThat(config.getProducerValueEvaluatorType()).isEqualTo(STRING);
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
    void shouldGetRecordEvaluatorType(String evaluatorType) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_RECORD_KEY_EVALUATOR_TYPE, evaluatorType);
        params.put(GlobalConfig.PRODUCER_RECORD_VALUE_EVALUATOR_TYPE, evaluatorType);
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.getProducerKeyEvaluatorType())
                .isEqualTo(EvaluatorType.valueOf(evaluatorType));
        assertThat(config.getProducerValueEvaluatorType())
                .isEqualTo(EvaluatorType.valueOf(evaluatorType));
    }

    @Test
    void shouldGetDestinationMappingWithOneTopic() {
        Map<String, String> params = producerEnabled();
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        List<DestinationMappingConfig> destinationMapping = config.getDestinationMapping();
        assertThat(destinationMapping).hasSize(1);

        DestinationMappingConfig mappingConfig = destinationMapping.get(0);
        assertThat(mappingConfig.destination()).isEqualTo("destination");
        assertThat(mappingConfig.topics()).containsExactly("test-topic");
    }

    @Test
    void shouldGetDestinationMappingWithMoreTopics() {
        Map<String, String> params = producerEnabled();
        params.put("producer.map.destination.to", "test-topic1,test-topic2");
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        List<DestinationMappingConfig> destinationMapping = config.getDestinationMapping();
        assertThat(destinationMapping).hasSize(1);

        DestinationMappingConfig mappingConfig = destinationMapping.get(0);
        assertThat(mappingConfig.destination()).isEqualTo("destination");
        assertThat(mappingConfig.topics()).containsExactly("test-topic1", "test-topic2");
    }

    @Test
    void shouldGetMoreDestinationMappings() {
        Map<String, String> params = producerEnabled();
        params.put("producer.map.destination.to", "test-topic1,test-topic2");
        params.put("producer.map.anotherDestination.to", "test-topic3");
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        List<DestinationMappingConfig> destinationMapping = config.getDestinationMapping();
        assertThat(destinationMapping).hasSize(2);

        DestinationMappingConfig mappingConfig1 = destinationMapping.get(0);
        assertThat(mappingConfig1.destination()).isEqualTo("destination");
        assertThat(mappingConfig1.topics()).containsExactly("test-topic1", "test-topic2");

        DestinationMappingConfig mappingConfig2 = destinationMapping.get(1);
        assertThat(mappingConfig2.destination()).isEqualTo("anotherDestination");
        assertThat(mappingConfig2.topics()).containsExactly("test-topic3");
    }

    @Test
    void shouldRetrieveBaseProducerProperties() {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), producerEnabled());
        assertThat(config.baseProducerProps())
                .containsExactly(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000",
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
                        ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE),
                        ProducerConfig.LINGER_MS_CONFIG, "0",
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000",
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "1", "all", "-1"})
    void shouldGetOverriddenProducerAcks(String acks) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_ACKS, acks);
        params.put(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE, "false");

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps()).containsEntry(ProducerConfig.ACKS_CONFIG, acks);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-2", "2"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerAcks(String acks) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_ACKS, acks);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_ACKS));
    }

    @Test
    void shouldGetOverriddenEnableIdempotence() {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE, "false");

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps())
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
    }

    @Test
    void shouldGetEnabledIdempotenceWithCompatibleAcks() {
        Map<String, String> params = producerEnabled();

        String[] acks = {"-1", "all"};
        for (String ack : acks) {
            params.put(GlobalConfig.PRODUCER_ACKS, ack);
            assertDoesNotThrow(() -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        }

        String[] notAllowedAcks = {"0", "1"};
        for (String ack : notAllowedAcks) {
            params.put(GlobalConfig.PRODUCER_ACKS, ack);
            ConfigException ce =
                    assertThrows(
                            ConfigException.class,
                            () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Parameter [%s] set to true is not compatible with parameter [%s] set to %s"
                                    .formatted(
                                            GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE,
                                            GlobalConfig.PRODUCER_ACKS,
                                            ack));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid"})
    @NullAndEmptySource
    void shouldFailDueToInvalidEnableIdempotence(String invalidValue) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE, invalidValue);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "5"})
    void shouldGetOverriddenProducerRetries(String retries) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE, "false");
        params.put(GlobalConfig.PRODUCER_RETRIES, retries);

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps())
                .containsEntry(ProducerConfig.RETRIES_CONFIG, retries);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerRetries(String invalidValue) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_RETRIES, invalidValue);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_RETRIES));
    }

    @Test
    void shouldFailDueToEnabledIdempotenceWithZeroRetries() {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE, "true");
        params.put(GlobalConfig.PRODUCER_RETRIES, "0");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Parameter [%s] set to true is not compatible with parameter [%s] set to 0"
                                .formatted(
                                        GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE,
                                        GlobalConfig.PRODUCER_RETRIES));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "50"})
    void shouldGetOverriddenLinger(String linger) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_LINGER_MS, linger);

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps())
                .containsEntry(ProducerConfig.LINGER_MS_CONFIG, linger);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerLinger(String invalidValue) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_LINGER_MS, invalidValue);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_LINGER_MS));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "50"})
    void shouldGetOverriddenDeliveryTimeout(String deliveryTimeout) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_DELIVERY_TIMEOUT_MS, deliveryTimeout);
        params.put(GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS, "0");
        params.put(GlobalConfig.PRODUCER_LINGER_MS, "0");

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps())
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerDeliveryTimeout(String invalidValue) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_DELIVERY_TIMEOUT_MS, invalidValue);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_DELIVERY_TIMEOUT_MS));
    }

    @Test
    void shouldFailDueToProducerDeliverTimeoutLessThanLingerPlusRequest() {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_DELIVERY_TIMEOUT_MS, "100");
        params.put(GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS, "50");
        params.put(GlobalConfig.PRODUCER_LINGER_MS, "60");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Parameter [%s] must be at least the sum of [%s] and [%s]"
                                .formatted(
                                        GlobalConfig.PRODUCER_DELIVERY_TIMEOUT_MS,
                                        GlobalConfig.PRODUCER_LINGER_MS,
                                        GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS));
    }

    @Test
    void shouldAcceptDeliveryTimeoutEqualToLingerPlusRequest() {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_DELIVERY_TIMEOUT_MS, "110");
        params.put(GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS, "50");
        params.put(GlobalConfig.PRODUCER_LINGER_MS, "60");

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps())
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "110");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "50"})
    void shouldGetOverriddenRequestTimeout(String requestTimeout) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS, requestTimeout);

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.baseProducerProps())
                .containsEntry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidRequestTimeout(String invalidValue) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS, invalidValue);

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS));
    }

    @Test
    void shouldGetDefaultSendConfirmationMode() {
        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), producerEnabled());
        assertThat(config.getProducerSendConfirmationMode())
                .isEqualTo(SendConfirmationMode.CONFIRMED);
    }

    static Stream<Arguments> sendConfirmationModes() {
        return Stream.of(
                Arguments.of(SendConfirmationMode.IMMEDIATE, "0"),
                Arguments.of(SendConfirmationMode.IMMEDIATE, "1"),
                Arguments.of(SendConfirmationMode.IMMEDIATE, "-1"),
                Arguments.of(SendConfirmationMode.IMMEDIATE, "all"),
                Arguments.of(SendConfirmationMode.CONFIRMED, "0"),
                Arguments.of(SendConfirmationMode.CONFIRMED, "1"),
                Arguments.of(SendConfirmationMode.CONFIRMED, "-1"),
                Arguments.of(SendConfirmationMode.CONFIRMED, "all"),
                Arguments.of(SendConfirmationMode.DETAILED, "-1"),
                Arguments.of(SendConfirmationMode.DETAILED, "1"),
                Arguments.of(SendConfirmationMode.DETAILED, "all"));
    }

    @ParameterizedTest
    @MethodSource("sendConfirmationModes")
    void shouldGetSendConfirmationMode(SendConfirmationMode mode, String acksString) {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE, "false");
        params.put(GlobalConfig.PRODUCER_ACKS, acksString);
        params.put(GlobalConfig.PRODUCER_SEND_CONFIRMATION_MODE, mode.name());

        GlobalConfig config = GlobalConfig.newConfig(adapterDir.toFile(), params);
        assertThat(config.getProducerSendConfirmationMode()).isEqualTo(mode);
    }

    @Test
    void shouldFailDueToInvalidSendConfirmationMode() {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_SEND_CONFIRMATION_MODE, "INVALID_MODE");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(GlobalConfig.PRODUCER_SEND_CONFIRMATION_MODE));
    }

    @Test
    void shouldFailDueToDetailedSendConfirmationModeWithProducerAcksZero() {
        Map<String, String> params = producerEnabled();
        params.put(GlobalConfig.PRODUCER_ACKS, "0");
        params.put(GlobalConfig.PRODUCER_SEND_CONFIRMATION_MODE, "DETAILED");

        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () -> GlobalConfig.newConfig(adapterDir.toFile(), params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Parameter [%s] set to %s is not compatible with parameter [%s] set to 0"
                                .formatted(
                                        GlobalConfig.PRODUCER_SEND_CONFIRMATION_MODE,
                                        SendConfirmationMode.DETAILED,
                                        GlobalConfig.PRODUCER_ACKS));
    }
}
