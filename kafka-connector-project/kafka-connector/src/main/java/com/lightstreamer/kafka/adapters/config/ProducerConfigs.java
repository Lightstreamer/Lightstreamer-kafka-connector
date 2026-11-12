
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.STRING;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.ACKS;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.HOST_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.PAYLOAD_CONVERTER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.POSITIVE_LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class ProducerConfigs {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String MESSAGE_KEY_PAYLOAD_CONVERTER = "message.key.payload.converter";
    public static final String MESSAGE_VALUE_PAYLOAD_CONVERTER = "message.value.payload.converter";
    public static final String DESTINATION_MAPPING = "map";
    public static final String MAP_TO_TOPICS_SUFFIX = "to";
    public static final String SEND_CONFIRMATION_MODE = "send.confirmation.mode";
    public static final String RECORD_ACKS = "acks";
    public static final String IDEMPOTENCE_ENABLE = "idempotence.enable";
    public static final String RETRIES = "retries";
    public static final String LINGER_MS = "linger.ms";
    public static final String DELIVERY_TIMEOUT_MS = "delivery.timeout.ms";
    public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";

    // Hidden (undocumented) producer config
    public static final String MAX_BLOCK_MS = "max.block.ms";
    public static final String METADATA_MAX_IDLE_MS = "metadata.max.idle.ms";

    private static final ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("producer")
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
                        .add(
                                SEND_CONFIRMATION_MODE,
                                false,
                                false,
                                CONFIRMATION_MODE,
                                defaultValue(SendConfirmationMode.CONFIRMED.toString()))
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

    static Properties addBaseProperties(GlobalConfig globalConfig) {
        NonNullKeyProperties properties = new NonNullKeyProperties();
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                globalConfig.getHostsList(GlobalConfig.PRODUCER_BOOTSTRAP_SERVERS));
        properties.setProperty(
                ProducerConfig.MAX_BLOCK_MS_CONFIG,
                globalConfig.getNonNegativeLongStr(GlobalConfig.PRODUCER_MAX_BLOCK_MS));
        properties.setProperty(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                globalConfig.getBooleanStr(GlobalConfig.PRODUCER_IDEMPOTENCE_ENABLE));
        properties.setProperty(
                ProducerConfig.RETRIES_CONFIG,
                globalConfig.getNonNegativeIntStr(GlobalConfig.PRODUCER_RETRIES));
        properties.setProperty(
                ProducerConfig.LINGER_MS_CONFIG,
                globalConfig.getNonNegativeLongStr(GlobalConfig.PRODUCER_LINGER_MS));
        properties.setProperty(
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                globalConfig.getNonNegativeLongStr(GlobalConfig.PRODUCER_DELIVERY_TIMEOUT_MS));
        properties.setProperty(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                globalConfig.getNonNegativeIntStr(GlobalConfig.PRODUCER_REQUEST_TIMEOUT_MS));
        properties.setProperty(
                ProducerConfig.METADATA_MAX_IDLE_CONFIG,
                globalConfig.getPositiveLongStr(GlobalConfig.PRODUCER_METADATA_MAX_IDLE_MS));
        properties.setProperty(
                ProducerConfig.ACKS_CONFIG,
                globalConfig.get(GlobalConfig.PRODUCER_ACKS, ACKS, false));
        return properties.unmodifiable();
    }
}
