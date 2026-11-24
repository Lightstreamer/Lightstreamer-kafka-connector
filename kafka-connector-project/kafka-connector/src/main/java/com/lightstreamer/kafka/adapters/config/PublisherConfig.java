
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType.STRING;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.ACKS;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.HOST_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.PAYLOAD_CONVERTER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.POSITIVE_LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.google.common.annotations.VisibleForTesting;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.PayloadConverterType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.DestinationConfigurations;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

public final class PublisherConfig extends AbstractConfig {

    public static final String BOOTSTRAP_SERVERS = adapt("bootstrap.servers");

    public static final String MESSAGE_KEY_PAYLOAD_CONVERTER =
            adapt("message.key.payload.converter.type");
    public static final String MESSAGE_VALUE_PAYLOAD_CONVERTER =
            adapt("message.value.payload.converter.type");
    public static final String DESTINATION_MAPPING = adapt("map");
    public static final String MAP_TO_TOPICS_SUFFIX = "to";
    public static final String SEND_CONFIRMATION_ENABLE = adapt("send.confirmation.enable");
    public static final String RECORD_ACKS = adapt("acks");
    public static final String IDEMPOTENCE_ENABLE = adapt("idempotence.enable");
    public static final String RETRIES = adapt("retries");
    public static final String LINGER_MS = adapt("linger.ms");
    public static final String DELIVERY_TIMEOUT_MS = adapt("delivery.timeout.ms");
    public static final String REQUEST_TIMEOUT_MS = adapt("request.timeout.ms");

    // Hidden (undocumented) publisher config
    public static final String MAX_BLOCK_MS = adapt("max.block.ms");
    public static final String METADATA_MAX_IDLE_MS = adapt("metadata.max.idle.ms");

    public static final String NAME_SPACE = "publisher";

    static String adapt(String key) {
        return NAME_SPACE + "." + key;
    }

    private static final ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("publisher")
                        .add(BOOTSTRAP_SERVERS, true, false, HOST_LIST)
                        .add(
                                MESSAGE_KEY_PAYLOAD_CONVERTER,
                                false,
                                false,
                                PAYLOAD_CONVERTER,
                                defaultValue(STRING.toString()))
                        .add(
                                MESSAGE_VALUE_PAYLOAD_CONVERTER,
                                false,
                                false,
                                PAYLOAD_CONVERTER,
                                defaultValue(STRING.toString()))
                        .add(RECORD_ACKS, false, false, ACKS, defaultValue("all"))
                        .add(SEND_CONFIRMATION_ENABLE, false, false, BOOL, defaultValue("true"))
                        .add(DESTINATION_MAPPING, true, true, MAP_TO_TOPICS_SUFFIX, TEXT_LIST)
                        .add(IDEMPOTENCE_ENABLE, false, false, BOOL, defaultValue("true"))
                        .add(
                                RETRIES,
                                false,
                                false,
                                NON_NEGATIVE_INT,
                                defaultValue(Integer.toString(Integer.MAX_VALUE)))
                        .add(LINGER_MS, false, false, NON_NEGATIVE_LONG, defaultValue("0"))
                        .add(
                                DELIVERY_TIMEOUT_MS,
                                false,
                                false,
                                NON_NEGATIVE_LONG,
                                defaultValue("5000"))
                        .add(
                                REQUEST_TIMEOUT_MS,
                                false,
                                false,
                                NON_NEGATIVE_INT,
                                defaultValue("2000"))
                        .add(MAX_BLOCK_MS, false, false, NON_NEGATIVE_LONG, defaultValue("100"))
                        .add(
                                METADATA_MAX_IDLE_MS,
                                false,
                                false,
                                POSITIVE_LONG,
                                defaultValue("3600000"));
    }

    static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    private Map<String, Object> addBaseConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getHostsList(BOOTSTRAP_SERVERS));
        configs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, getNonNegativeLongStr(MAX_BLOCK_MS));
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, getBooleanStr(IDEMPOTENCE_ENABLE));
        configs.put(ProducerConfig.RETRIES_CONFIG, getNonNegativeIntStr(RETRIES));
        configs.put(ProducerConfig.LINGER_MS_CONFIG, getNonNegativeLongStr(LINGER_MS));
        configs.put(
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                getNonNegativeLongStr(DELIVERY_TIMEOUT_MS));
        configs.put(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, getNonNegativeIntStr(REQUEST_TIMEOUT_MS));
        configs.put(
                ProducerConfig.METADATA_MAX_IDLE_CONFIG, getPositiveLongStr(METADATA_MAX_IDLE_MS));
        configs.put(ProducerConfig.ACKS_CONFIG, get(RECORD_ACKS, ACKS, false));
        return configs;
    }

    private DestinationConfigurations destinationConfigurations;

    private PublisherConfig(ConfigsSpec spec, Map<String, String> configs) {
        super(spec, configs);
    }

    public PublisherConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);

        Map<String, String> values = getValues(DESTINATION_MAPPING, MAP_TO_TOPICS_SUFFIX);
        this.destinationConfigurations = DestinationConfigurations.from(values);

        postValidate();
    }

    private void postValidate() {
        String acks = get(RECORD_ACKS, ACKS, false);
        checkEnableIdempotence(acks);
        checkProducerDeliveryTimeout();
    }

    private void checkEnableIdempotence(String acks) {
        boolean enableIdempotence = getBoolean(IDEMPOTENCE_ENABLE);
        if (enableIdempotence) {
            if (!(acks.equals("all") || acks.equals("-1"))) {
                throw new ConfigException(
                        "Parameter [%s] set to true is not compatible with parameter [%s] set to %s"
                                .formatted(IDEMPOTENCE_ENABLE, RECORD_ACKS, acks));
            }

            if (getNonNegativeInt(RETRIES) == 0) {
                throw new ConfigException(
                        "Parameter [%s] set to true is not compatible with parameter [%s] set to 0"
                                .formatted(IDEMPOTENCE_ENABLE, RETRIES));
            }
        }
    }

    private void checkProducerDeliveryTimeout() {
        long linger = getNonNegativeLong(LINGER_MS);
        long requestTimeout = getNonNegativeInt(REQUEST_TIMEOUT_MS);
        long deliveryTimeout = getNonNegativeLong(DELIVERY_TIMEOUT_MS);
        if (deliveryTimeout < linger + requestTimeout) {
            throw new ConfigException(
                    "Parameter [%s] must be at least the sum of [%s] and [%s]"
                            .formatted(DELIVERY_TIMEOUT_MS, LINGER_MS, REQUEST_TIMEOUT_MS));
        }
    }

    public PayloadConverterType getMessageKeyPayloadConverter() {
        return PayloadConverterType.valueOf(
                get(MESSAGE_KEY_PAYLOAD_CONVERTER, PAYLOAD_CONVERTER, false));
    }

    public PayloadConverterType getMessageValuePayloadConverter() {
        return PayloadConverterType.valueOf(
                get(MESSAGE_VALUE_PAYLOAD_CONVERTER, PAYLOAD_CONVERTER, false));
    }

    public boolean isSendConfirmationEnable() {
        return getBoolean(SEND_CONFIRMATION_ENABLE);
    }

    public Map<String, Object> producerConfigs() {
        return Map.copyOf(addBaseConfigs());
    }

    public DestinationConfigurations getDestinationConfigurations() {

        return destinationConfigurations;
    }

    @VisibleForTesting
    public static PublisherConfig defaultConfig() {
        return new PublisherConfig(
                Map.of(
                        BOOTSTRAP_SERVERS,
                        "localhost:9092",
                        "publisher.map.destination.to",
                        "topic"));
    }

    @VisibleForTesting
    public static PublisherConfig defaultConfig(String... kvPairs) {
        if (kvPairs.length % 2 != 0) {
            throw new IllegalArgumentException("Expected even number of key-value pairs");
        }
        Map<String, String> config =
                new HashMap<>(
                        Map.of(
                                BOOTSTRAP_SERVERS,
                                "localhost:9092",
                                "publisher.map.destination.to",
                                "topic"));
        for (int i = 0; i < kvPairs.length - 1; i += 2) {
            config.put(kvPairs[i], kvPairs[i + 1]);
        }
        return new PublisherConfig(config);
    }
}
