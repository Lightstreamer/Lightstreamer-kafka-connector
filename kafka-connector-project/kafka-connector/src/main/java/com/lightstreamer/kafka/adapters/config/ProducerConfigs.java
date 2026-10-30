
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.HOST_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.POSITIVE_LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class ProducerConfigs {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String RECORD_KEY_EVALUATOR_TYPE = "record.key.evaluator.type";
    public static final String RECORD_VALUE_EVALUATOR_TYPE = "record.value.evaluator.type";
    public static final String SEND_CONFIRMATION_MODE = "send.confirmation.mode";
    public static final String RECORD_ACKS = "acks";

    // Hidden (undocumented) producer config
    public static final String MAX_BLOCK_MS = "max.block.ms";

    private static final ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("producer")
                        .add(BOOTSTRAP_SERVERS, true, false, HOST_LIST)
                        .add(
                                RECORD_KEY_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(STRING.toString()))
                        .add(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(STRING.toString()))
                        .add(RECORD_ACKS, false, false, ACKS, defaultValue("all"))
                        .add(
                                SEND_CONFIRMATION_MODE,
                                false,
                                false,
                                CONFIRMATION_MODE,
                                defaultValue(SendConfirmationMode.CONFIRMED.toString()))
                        .add(MAX_BLOCK_MS, false, false, POSITIVE_LONG, defaultValue("1000"));
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
                globalConfig.getPositiveLong(GlobalConfig.PRODUCER_MAX_BLOCK_MS));
        properties.setProperty(
                ProducerConfig.ACKS_CONFIG,
                globalConfig.get(GlobalConfig.PRODUCER_ACKS, ACKS, false));
        return properties.unmodifiable();
    }
}
