
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.HOST_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.common.config.ConfigException;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public final class GlobalConfig extends AbstractConfig {

    public static final String LOGGING_CONFIGURATION_PATH = "logging.configuration.path";

    public static final String PUBLISH_NAME_SPACE = "publish";
    public static final String PUBLISH_ENABLE = "publish.enable";
    public static final String PUBLISH_BOOTSTRAP_SERVERS = adapt(PublishConfigs.BOOTSTRAP_SERVERS);
    public static final String PUBLISH_MAX_BLOCK_MS = adapt(PublishConfigs.MAX_BLOCK_MS);
    public static final String PUBLISH_RECORD_KEY_EVALUATOR_TYPE =
            adapt(PublishConfigs.RECORD_KEY_EVALUATOR_TYPE);
    public static final String PUBLISH_RECORD_VALUE_EVALUATOR_TYPE =
            adapt(PublishConfigs.RECORD_VALUE_EVALUATOR_TYPE);
    public static final String PUBLISH_ACK_ENABLED = adapt(PublishConfigs.ACK);
    public static final String PUBLISH_ACK = adapt(PublishConfigs.ACK);

    private static final ConfigsSpec CONFIG_SPEC;

    static String adapt(String key) {
        return PUBLISH_NAME_SPACE + "." + key;
    }

    static {
        CONFIG_SPEC =
                new ConfigsSpec()
                        .add(ADAPTERS_CONF_ID, true, false, TEXT)
                        .add(LOGGING_CONFIGURATION_PATH, true, false, FILE)
                        .add(
                                PUBLISH_ENABLE,
                                true,
                                false,
                                ConfigsSpec.ConfType.BOOL,
                                defaultValue("false"))
                        .withEnabledChildConfigs(
                                PublishConfigs.spec().newSpecWithNameSpace(PUBLISH_NAME_SPACE),
                                PUBLISH_ENABLE);
    }

    private GlobalConfig(ConfigsSpec spec, Map<String, String> configs) {
        super(spec, configs);
    }

    public GlobalConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);
    }

    static ConfigsSpec configSpec() {
        return CONFIG_SPEC;
    }

    public static GlobalConfig newConfig(File adapterDir, Map<String, String> params) {
        Map<String, String> updatedConfigs =
                AbstractConfig.resolveFilePaths(CONFIG_SPEC, params, adapterDir);
        return new GlobalConfig(updatedConfigs);
    }

    public boolean isPublishEnabled() {
        return getBoolean(PUBLISH_ENABLE);
    }

    public boolean isPublishAckEnabled() {
        checkPublishEnabled();
        return getBoolean(PUBLISH_ACK_ENABLED);
    }

    private void checkPublishEnabled() {
        if (!isPublishEnabled()) {
            throw new ConfigException(
                    "Publish is not enabled. Check parameter [%s]".formatted(PUBLISH_ENABLE));
        }
    }

    public String getPublishBootstrapServers() {
        checkPublishEnabled();
        return get(PUBLISH_BOOTSTRAP_SERVERS, HOST_LIST, false);
    }

    public EvaluatorType getPublishKeyEvaluatorType() {
        checkPublishEnabled();
        return EvaluatorType.valueOf(get(PUBLISH_RECORD_KEY_EVALUATOR_TYPE, EVALUATOR, false));
    }

    public EvaluatorType getPublishValueEvaluatorType() {
        checkPublishEnabled();
        return EvaluatorType.valueOf(get(PUBLISH_RECORD_VALUE_EVALUATOR_TYPE, EVALUATOR, false));
    }

    public Properties baseProducerProps() {
        checkPublishEnabled();
        NonNullKeyProperties properties = new NonNullKeyProperties();
        properties.putAll(PublishConfigs.addBaseProperties(this));
        return properties.unmodifiable();
    }
}
