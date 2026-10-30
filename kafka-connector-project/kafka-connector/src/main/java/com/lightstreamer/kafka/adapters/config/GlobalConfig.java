
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.ACKS;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CONFIRMATION_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SendConfirmationMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.common.config.ConfigException;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public final class GlobalConfig extends AbstractConfig {

    public static final String LOGGING_CONFIGURATION_PATH = "logging.configuration.path";

    public static final String PRODUCER_NAME_SPACE = "producer";
    public static final String PRODUCER_ENABLE = "producer.enable";
    public static final String PRODUCER_BOOTSTRAP_SERVERS =
            adapt(ProducerConfigs.BOOTSTRAP_SERVERS);
    public static final String PRODUCER_MAX_BLOCK_MS = adapt(ProducerConfigs.MAX_BLOCK_MS);
    public static final String PRODUCER_RECORD_KEY_EVALUATOR_TYPE =
            adapt(ProducerConfigs.RECORD_KEY_EVALUATOR_TYPE);
    public static final String PRODUCER_RECORD_VALUE_EVALUATOR_TYPE =
            adapt(ProducerConfigs.RECORD_VALUE_EVALUATOR_TYPE);
    public static final String PRODUCER_SEND_CONFIRMATION_MODE =
            adapt(ProducerConfigs.SEND_CONFIRMATION_MODE);
    public static final String PRODUCER_ACKS = adapt(ProducerConfigs.RECORD_ACKS);

    private static final ConfigsSpec CONFIG_SPEC;

    static String adapt(String key) {
        return PRODUCER_NAME_SPACE + "." + key;
    }

    static {
        CONFIG_SPEC =
                new ConfigsSpec()
                        .add(ADAPTERS_CONF_ID, true, false, TEXT)
                        .add(LOGGING_CONFIGURATION_PATH, true, false, FILE)
                        .add(PRODUCER_ENABLE, true, false, BOOL, defaultValue("false"))
                        .withEnabledChildConfigs(
                                ProducerConfigs.spec().newSpecWithNameSpace(PRODUCER_NAME_SPACE),
                                PRODUCER_ENABLE);
    }

    private GlobalConfig(ConfigsSpec spec, Map<String, String> configs) {
        super(spec, configs);
    }

    public GlobalConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);
        postValidate();
    }

    private void postValidate() {
        if (!isProducerEnabled()) {
            return;
        }

        SendConfirmationMode confirmationMode = getProducerSendConfirmationMode();
        if (confirmationMode == SendConfirmationMode.DETAILED
                && get(PRODUCER_ACKS, ACKS, false).equals("0")) {
            throw new ConfigException(
                    "Parameter [%s] set to %s is not compatible with parameter [%s] set to 0"
                            .formatted(
                                    PRODUCER_SEND_CONFIRMATION_MODE,
                                    SendConfirmationMode.DETAILED,
                                    PRODUCER_ACKS));
        }
    }

    static ConfigsSpec configSpec() {
        return CONFIG_SPEC;
    }

    public static GlobalConfig newConfig(File adapterDir, Map<String, String> params) {
        Map<String, String> updatedConfigs =
                AbstractConfig.resolveFilePaths(CONFIG_SPEC, params, adapterDir);
        return new GlobalConfig(updatedConfigs);
    }

    public boolean isProducerEnabled() {
        return getBoolean(PRODUCER_ENABLE);
    }

    private void checkProducerEnabled() {
        if (!isProducerEnabled()) {
            throw new ConfigException(
                    "Producer is not enabled. Check parameter [%s]".formatted(PRODUCER_ENABLE));
        }
    }

    public EvaluatorType getProducerKeyEvaluatorType() {
        checkProducerEnabled();
        return EvaluatorType.valueOf(get(PRODUCER_RECORD_KEY_EVALUATOR_TYPE, EVALUATOR, false));
    }

    public EvaluatorType getProducerValueEvaluatorType() {
        checkProducerEnabled();
        return EvaluatorType.valueOf(get(PRODUCER_RECORD_VALUE_EVALUATOR_TYPE, EVALUATOR, false));
    }

    public SendConfirmationMode getProducerSendConfirmationMode() {
        checkProducerEnabled();
        return SendConfirmationMode.valueOf(
                get(PRODUCER_SEND_CONFIRMATION_MODE, CONFIRMATION_MODE, false));
    }

    public Properties baseProducerProps() {
        checkProducerEnabled();
        NonNullKeyProperties properties = new NonNullKeyProperties();
        properties.putAll(ProducerConfigs.addBaseProperties(this));
        return properties.unmodifiable();
    }
}
