
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.common.config.ConfigException;

import java.io.File;
import java.util.Map;

public final class GlobalConfig extends AbstractConfig {

    public static final String LOGGING_CONFIGURATION_PATH = "logging.configuration.path";
    public static final String PUBLISHER_ENABLE = "publisher.enable";

    private static final ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec()
                        .add(ADAPTERS_CONF_ID, true, false, TEXT)
                        .add(LOGGING_CONFIGURATION_PATH, true, false, FILE)
                        .add(PUBLISHER_ENABLE, true, false, BOOL, defaultValue("false"))
                        .withEnabledChildConfigs(PublisherConfig.spec(), PUBLISHER_ENABLE);
    }

    private PublisherConfig publisherConfig;

    private GlobalConfig(ConfigsSpec spec, Map<String, String> configs) {
        super(spec, configs);
    }

    public GlobalConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);

        if (isPublisherEnabled()) {
            this.publisherConfig = new PublisherConfig(configs);
        }
        postValidate();
    }

    private void postValidate() {
        if (!isPublisherEnabled()) {
            return;
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

    public boolean isPublisherEnabled() {
        return getBoolean(PUBLISHER_ENABLE);
    }

    private void checkPublisherEnabled() {
        if (!isPublisherEnabled()) {
            throw new ConfigException(
                    "Publisher is not enabled. Check parameter [%s]".formatted(PUBLISHER_ENABLE));
        }
    }

    public PublisherConfig getPublisherConfig() {
        checkPublisherEnabled();
        return publisherConfig;
    }
}
