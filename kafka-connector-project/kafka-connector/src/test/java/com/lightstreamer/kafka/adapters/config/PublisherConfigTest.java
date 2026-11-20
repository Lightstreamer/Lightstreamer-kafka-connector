
/*
 * Copyright (C) 2026 Lightstreamer Srl
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
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.BOOTSTRAP_SERVERS;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.DELIVERY_TIMEOUT_MS;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.DESTINATION_MAPPING;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.IDEMPOTENCE_ENABLE;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.LINGER_MS;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.MAP_TO_TOPICS_SUFFIX;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.MAX_BLOCK_MS;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.MESSAGE_KEY_PAYLOAD_CONVERTER;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.MESSAGE_VALUE_PAYLOAD_CONVERTER;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.METADATA_MAX_IDLE_MS;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.RECORD_ACKS;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.REQUEST_TIMEOUT_MS;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.RETRIES;
import static com.lightstreamer.kafka.adapters.config.PublisherConfig.SEND_CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.HOST_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.PAYLOAD_CONVERTER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.POSITIVE_LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT_LIST;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.DestinationConfigurations;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

class PublisherConfigTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = PublisherConfig.spec();

        ConfParameter bootStrapServers = configSpec.findParameter(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.name()).isEqualTo(BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.required()).isTrue();
        assertThat(bootStrapServers.multiple()).isFalse();
        assertThat(bootStrapServers.mutable()).isTrue();
        assertThat(bootStrapServers.defaultValue()).isNull();
        assertThat(bootStrapServers.type()).isEqualTo(HOST_LIST);

        ConfParameter keyPayloadConverter = configSpec.findParameter(MESSAGE_KEY_PAYLOAD_CONVERTER);
        assertThat(keyPayloadConverter.name()).isEqualTo(MESSAGE_KEY_PAYLOAD_CONVERTER);
        assertThat(keyPayloadConverter.required()).isFalse();
        assertThat(keyPayloadConverter.multiple()).isFalse();
        assertThat(keyPayloadConverter.mutable()).isTrue();
        assertThat(keyPayloadConverter.defaultValue()).isEqualTo("STRING");
        assertThat(keyPayloadConverter.type()).isEqualTo(PAYLOAD_CONVERTER);

        ConfParameter valuePayloadConverter =
                configSpec.findParameter(MESSAGE_VALUE_PAYLOAD_CONVERTER);
        assertThat(valuePayloadConverter.name()).isEqualTo(MESSAGE_VALUE_PAYLOAD_CONVERTER);
        assertThat(valuePayloadConverter.required()).isFalse();
        assertThat(valuePayloadConverter.multiple()).isFalse();
        assertThat(valuePayloadConverter.mutable()).isTrue();
        assertThat(valuePayloadConverter.defaultValue()).isEqualTo("STRING");
        assertThat(valuePayloadConverter.type()).isEqualTo(PAYLOAD_CONVERTER);

        ConfParameter recordAcks = configSpec.findParameter(RECORD_ACKS);
        assertThat(recordAcks.name()).isEqualTo(RECORD_ACKS);
        assertThat(recordAcks.required()).isFalse();
        assertThat(recordAcks.multiple()).isFalse();
        assertThat(recordAcks.mutable()).isTrue();
        assertThat(recordAcks.defaultValue()).isEqualTo("all");
        assertThat(recordAcks.type()).isEqualTo(ConfType.ACKS);

        ConfParameter mapDestinationsTo =
                configSpec.findParameter(DESTINATION_MAPPING, MAP_TO_TOPICS_SUFFIX);
        assertThat(mapDestinationsTo.name()).isEqualTo(DESTINATION_MAPPING);
        assertThat(mapDestinationsTo.required()).isTrue();
        assertThat(mapDestinationsTo.multiple()).isTrue();
        assertThat(mapDestinationsTo.mutable()).isTrue();
        assertThat(mapDestinationsTo.defaultValue()).isNull();
        assertThat(mapDestinationsTo.type()).isEqualTo(TEXT_LIST);

        ConfParameter sendConfirmationMode = configSpec.findParameter(SEND_CONFIRMATION_MODE);
        assertThat(sendConfirmationMode.name()).isEqualTo(SEND_CONFIRMATION_MODE);
        assertThat(sendConfirmationMode.required()).isFalse();
        assertThat(sendConfirmationMode.multiple()).isFalse();
        assertThat(sendConfirmationMode.mutable()).isTrue();
        assertThat(sendConfirmationMode.defaultValue()).isEqualTo("CONFIRMED");
        assertThat(sendConfirmationMode.type()).isEqualTo(CONFIRMATION_MODE);

        ConfParameter enableIdempotence = configSpec.findParameter(IDEMPOTENCE_ENABLE);
        assertThat(enableIdempotence.name()).isEqualTo(IDEMPOTENCE_ENABLE);
        assertThat(enableIdempotence.required()).isFalse();
        assertThat(enableIdempotence.multiple()).isFalse();
        assertThat(enableIdempotence.mutable()).isTrue();
        assertThat(enableIdempotence.defaultValue()).isEqualTo("true");
        assertThat(enableIdempotence.type()).isEqualTo(ConfType.BOOL);

        ConfParameter retries = configSpec.findParameter(RETRIES);
        assertThat(retries.name()).isEqualTo(RETRIES);
        assertThat(retries.required()).isFalse();
        assertThat(retries.multiple()).isFalse();
        assertThat(retries.mutable()).isTrue();
        assertThat(retries.defaultValue()).isEqualTo(Integer.toString(Integer.MAX_VALUE));
        assertThat(retries.type()).isEqualTo(NON_NEGATIVE_INT);

        ConfParameter lingerMs = configSpec.findParameter(LINGER_MS);
        assertThat(lingerMs.name()).isEqualTo(LINGER_MS);
        assertThat(lingerMs.required()).isFalse();
        assertThat(lingerMs.multiple()).isFalse();
        assertThat(lingerMs.mutable()).isTrue();
        assertThat(lingerMs.defaultValue()).isEqualTo("0");
        assertThat(lingerMs.type()).isEqualTo(NON_NEGATIVE_LONG);

        ConfParameter deliverTimeoutMs = configSpec.findParameter(DELIVERY_TIMEOUT_MS);
        assertThat(deliverTimeoutMs.name()).isEqualTo(DELIVERY_TIMEOUT_MS);
        assertThat(deliverTimeoutMs.required()).isFalse();
        assertThat(deliverTimeoutMs.multiple()).isFalse();
        assertThat(deliverTimeoutMs.mutable()).isTrue();
        assertThat(deliverTimeoutMs.defaultValue()).isEqualTo("5000");
        assertThat(deliverTimeoutMs.type()).isEqualTo(NON_NEGATIVE_LONG);

        ConfParameter requestTimeoutMs = configSpec.findParameter(REQUEST_TIMEOUT_MS);
        assertThat(requestTimeoutMs.name()).isEqualTo(REQUEST_TIMEOUT_MS);
        assertThat(requestTimeoutMs.required()).isFalse();
        assertThat(requestTimeoutMs.multiple()).isFalse();
        assertThat(requestTimeoutMs.mutable()).isTrue();
        assertThat(requestTimeoutMs.defaultValue()).isEqualTo("2000");
        assertThat(requestTimeoutMs.type()).isEqualTo(NON_NEGATIVE_INT);

        ConfParameter maxBlockMs = configSpec.findParameter(MAX_BLOCK_MS);
        assertThat(maxBlockMs.name()).isEqualTo(MAX_BLOCK_MS);
        assertThat(maxBlockMs.required()).isFalse();
        assertThat(maxBlockMs.multiple()).isFalse();
        assertThat(maxBlockMs.mutable()).isTrue();
        assertThat(maxBlockMs.defaultValue()).isEqualTo("100");
        assertThat(maxBlockMs.type()).isEqualTo(NON_NEGATIVE_LONG);

        ConfParameter maxMetadataMaxIdleMs = configSpec.findParameter(METADATA_MAX_IDLE_MS);
        assertThat(maxMetadataMaxIdleMs.name()).isEqualTo(METADATA_MAX_IDLE_MS);
        assertThat(maxMetadataMaxIdleMs.required()).isFalse();
        assertThat(maxMetadataMaxIdleMs.multiple()).isFalse();
        assertThat(maxMetadataMaxIdleMs.mutable()).isTrue();
        assertThat(maxMetadataMaxIdleMs.defaultValue()).isEqualTo("3600000");
        assertThat(maxMetadataMaxIdleMs.type()).isEqualTo(POSITIVE_LONG);
    }

    @Test
    void shouldSpecifyRequiredParameters() {
        Map<String, String> params = new HashMap<>();

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Missing required parameter [%s]"
                                .formatted(PublisherConfig.BOOTSTRAP_SERVERS));

        params.put(PublisherConfig.BOOTSTRAP_SERVERS, "");
        ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.BOOTSTRAP_SERVERS));

        params.put(PublisherConfig.BOOTSTRAP_SERVERS, "localhost:9092");
        ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify at least one parameter [%s.<...>.%s]"
                                .formatted(
                                        PublisherConfig.DESTINATION_MAPPING,
                                        PublisherConfig.MAP_TO_TOPICS_SUFFIX));

        params.put("publisher.map.destination.to", "test-topic");
        assertDoesNotThrow(() -> new PublisherConfig(params));
    }

    @Test
    void shouldGetDefaultMessagePayloadConverterType() {
        PublisherConfig config = new PublisherConfig(minimal());
        assertThat(config.getMessageKeyPayloadConverter())
                .isEqualTo(ConfigTypes.PayloadConverterType.STRING);
        assertThat(config.getMessageValuePayloadConverter())
                .isEqualTo(ConfigTypes.PayloadConverterType.STRING);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "AVRO",
                "PROTOBUF",
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
    void shouldGetKeyMessagePayloadConverterType(String converterType) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.MESSAGE_KEY_PAYLOAD_CONVERTER, converterType);
        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.getMessageKeyPayloadConverter())
                .isEqualTo(PayloadConverterType.valueOf(converterType));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "AVRO",
                "PROTOBUF",
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
    void shouldGetValueMessagePayloadConverterType(String converterType) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.MESSAGE_VALUE_PAYLOAD_CONVERTER, converterType);
        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.getMessageValuePayloadConverter())
                .isEqualTo(PayloadConverterType.valueOf(converterType));
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID"})
    @NullAndEmptySource
    void shouldFailDueToInvalidMessageKeyPayloadConverterType(String converterType) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.MESSAGE_KEY_PAYLOAD_CONVERTER, converterType);
        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [publisher.message.key.payload.converter.type]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID"})
    @NullAndEmptySource
    void shouldFailDueToInvalidMessageValuePayloadConverterType(String converterType) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.MESSAGE_VALUE_PAYLOAD_CONVERTER, converterType);
        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [publisher.message.value.payload.converter.type]");
    }

    @Test
    void shouldGetDestinationMappingWithOneTopic() {
        Map<String, String> params = minimal();
        PublisherConfig config = new PublisherConfig(params);
        DestinationConfigurations destinationMapping = config.getDestinationConfigurations();

        assertThat(destinationMapping.size()).isEqualTo(1);
        assertThat(destinationMapping.lookupTopics("destination")).containsExactly("test-topic");
    }

    @Test
    void shouldGetDestinationMappingWithMoreTopics() {
        Map<String, String> params = minimal();
        params.put("publisher.map.destination.to", "test-topic1,test-topic2");
        PublisherConfig config = new PublisherConfig(params);
        DestinationConfigurations destinationMapping = config.getDestinationConfigurations();
        assertThat(destinationMapping.size()).isEqualTo(1);
        assertThat(destinationMapping.lookupTopics("destination"))
                .containsExactly("test-topic1", "test-topic2");
    }

    @Test
    void shouldGetMoreDestinationMappings() {
        Map<String, String> params = minimal();
        params.put("publisher.map.destination.to", "test-topic1,test-topic2");
        params.put("publisher.map.anotherDestination.to", "test-topic3");
        PublisherConfig config = new PublisherConfig(params);
        DestinationConfigurations destinationMapping = config.getDestinationConfigurations();
        assertThat(destinationMapping.size()).isEqualTo(2);
        assertThat(destinationMapping.lookupTopics("destination"))
                .containsExactly("test-topic1", "test-topic2");
        assertThat(destinationMapping.lookupTopics("anotherDestination"))
                .containsExactly("test-topic3");
    }

    @Test
    void shouldRetrieveBaseProducerProperties() {
        PublisherConfig config = new PublisherConfig(minimal());
        assertThat(config.producerConfigs())
                .containsExactly(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ProducerConfig.MAX_BLOCK_MS_CONFIG, "100",
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
                        ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE),
                        ProducerConfig.LINGER_MS_CONFIG, "0",
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000",
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000",
                        ProducerConfig.METADATA_MAX_IDLE_CONFIG, "3600000");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "1", "all", "-1"})
    void shouldGetOverriddenProducerAcks(String acks) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.RECORD_ACKS, acks);
        params.put(PublisherConfig.IDEMPOTENCE_ENABLE, "false");

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.producerConfigs()).containsEntry(ProducerConfig.ACKS_CONFIG, acks);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-2", "2"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerAcks(String acks) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.RECORD_ACKS, acks);

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.RECORD_ACKS));
    }

    @Test
    void shouldGetOverriddenEnableIdempotence() {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.IDEMPOTENCE_ENABLE, "false");

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.producerConfigs())
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
    }

    @Test
    void shouldGetEnabledIdempotenceWithCompatibleAcks() {
        Map<String, String> params = minimal();

        String[] acks = {"-1", "all"};
        for (String ack : acks) {
            params.put(PublisherConfig.RECORD_ACKS, ack);
            assertDoesNotThrow(() -> new PublisherConfig(params));
        }

        String[] notAllowedAcks = {"0", "1"};
        for (String ack : notAllowedAcks) {
            params.put(PublisherConfig.RECORD_ACKS, ack);
            ConfigException ce =
                    assertThrows(ConfigException.class, () -> new PublisherConfig(params));
            assertThat(ce)
                    .hasMessageThat()
                    .isEqualTo(
                            "Parameter [%s] set to true is not compatible with parameter [%s] set to %s"
                                    .formatted(
                                            PublisherConfig.IDEMPOTENCE_ENABLE,
                                            PublisherConfig.RECORD_ACKS,
                                            ack));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid"})
    @NullAndEmptySource
    void shouldFailDueToInvalidEnableIdempotence(String invalidValue) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.IDEMPOTENCE_ENABLE, invalidValue);

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.IDEMPOTENCE_ENABLE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "5"})
    void shouldGetOverriddenProducerRetries(String retries) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.IDEMPOTENCE_ENABLE, "false");
        params.put(PublisherConfig.RETRIES, retries);

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.producerConfigs()).containsEntry(ProducerConfig.RETRIES_CONFIG, retries);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerRetries(String invalidValue) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.RETRIES, invalidValue);

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.RETRIES));
    }

    @Test
    void shouldFailDueToEnabledIdempotenceWithZeroRetries() {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.IDEMPOTENCE_ENABLE, "true");
        params.put(PublisherConfig.RETRIES, "0");

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Parameter [%s] set to true is not compatible with parameter [%s] set to 0"
                                .formatted(
                                        PublisherConfig.IDEMPOTENCE_ENABLE,
                                        PublisherConfig.RETRIES));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "50"})
    void shouldGetOverriddenLinger(String linger) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.LINGER_MS, linger);

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.producerConfigs()).containsEntry(ProducerConfig.LINGER_MS_CONFIG, linger);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerLinger(String invalidValue) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.LINGER_MS, invalidValue);

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.LINGER_MS));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "50"})
    void shouldGetOverriddenDeliveryTimeout(String deliveryTimeout) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.DELIVERY_TIMEOUT_MS, deliveryTimeout);
        params.put(PublisherConfig.REQUEST_TIMEOUT_MS, "0");
        params.put(PublisherConfig.LINGER_MS, "0");

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.producerConfigs())
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidProducerDeliveryTimeout(String invalidValue) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.DELIVERY_TIMEOUT_MS, invalidValue);

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.DELIVERY_TIMEOUT_MS));
    }

    @Test
    void shouldFailDueToProducerDeliverTimeoutLessThanLingerPlusRequest() {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.DELIVERY_TIMEOUT_MS, "100");
        params.put(PublisherConfig.REQUEST_TIMEOUT_MS, "50");
        params.put(PublisherConfig.LINGER_MS, "60");

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Parameter [%s] must be at least the sum of [%s] and [%s]"
                                .formatted(
                                        PublisherConfig.DELIVERY_TIMEOUT_MS,
                                        PublisherConfig.LINGER_MS,
                                        PublisherConfig.REQUEST_TIMEOUT_MS));
    }

    @Test
    void shouldAcceptDeliveryTimeoutEqualToLingerPlusRequest() {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.DELIVERY_TIMEOUT_MS, "110");
        params.put(PublisherConfig.REQUEST_TIMEOUT_MS, "50");
        params.put(PublisherConfig.LINGER_MS, "60");

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.producerConfigs())
                .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "110");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "50"})
    void shouldGetOverriddenRequestTimeout(String requestTimeout) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.REQUEST_TIMEOUT_MS, requestTimeout);

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.producerConfigs())
                .containsEntry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "-1"})
    @NullAndEmptySource
    void shouldFailDueToInvalidRequestTimeout(String invalidValue) {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.REQUEST_TIMEOUT_MS, invalidValue);

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.REQUEST_TIMEOUT_MS));
    }

    @Test
    void shouldGetDefaultSendConfirmationMode() {
        PublisherConfig config = new PublisherConfig(minimal());
        assertThat(config.getSendConfirmationMode()).isEqualTo(SendConfirmationMode.CONFIRMED);
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
        Map<String, String> params = minimal();
        params.put(PublisherConfig.IDEMPOTENCE_ENABLE, "false");
        params.put(PublisherConfig.RECORD_ACKS, acksString);
        params.put(PublisherConfig.SEND_CONFIRMATION_MODE, mode.name());

        PublisherConfig config = new PublisherConfig(params);
        assertThat(config.getSendConfirmationMode()).isEqualTo(mode);
    }

    @Test
    void shouldFailDueToInvalidSendConfirmationMode() {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.SEND_CONFIRMATION_MODE, "INVALID_MODE");

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Specify a valid value for parameter [%s]"
                                .formatted(PublisherConfig.SEND_CONFIRMATION_MODE));
    }

    @Test
    void shouldFailDueToDetailedSendConfirmationModeWithProducerAcksZero() {
        Map<String, String> params = minimal();
        params.put(PublisherConfig.RECORD_ACKS, "0");
        params.put(PublisherConfig.SEND_CONFIRMATION_MODE, "DETAILED");

        ConfigException ce = assertThrows(ConfigException.class, () -> new PublisherConfig(params));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Parameter [%s] set to %s is not compatible with parameter [%s] set to 0"
                                .formatted(
                                        PublisherConfig.SEND_CONFIRMATION_MODE,
                                        SendConfirmationMode.DETAILED,
                                        PublisherConfig.RECORD_ACKS));
    }

    private Map<String, String> minimal() {
        Map<String, String> params = new HashMap<>();
        params.put(PublisherConfig.BOOTSTRAP_SERVERS, "localhost:9092");
        params.put("publisher.map.destination.to", "test-topic");
        return params;
    }
}
