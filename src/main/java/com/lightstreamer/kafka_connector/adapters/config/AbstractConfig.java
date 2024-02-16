
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

import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.HOST;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT_LIST;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.URL;

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.Type;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

abstract sealed class AbstractConfig permits GlobalConfig, ConnectorConfig {

    public static final String ADAPTERS_CONF_ID = "adapters_conf.id";
    public static final String ADAPTER_DIR = "adapter.dir";

    private final ConfigsSpec configSpec;
    private final Map<String, String> configuration;

    AbstractConfig(ConfigsSpec spec, Map<String, String> configs) {
        this.configSpec = spec;
        this.configuration = Collections.unmodifiableMap(this.configSpec.parse(configs));
        validate();
    }

    protected void validate() throws ConfigException {}

    public final Map<String, String> configuration() {
        return this.configuration;
    }

    public final String getInt(String configKey) {
        return get(configKey, ConfType.INT, false);
    }

    public final String getText(String configKey) {
        return get(configKey, TEXT, false);
    }

    public final String getText(String configKey, boolean forceRequired) {
        return get(configKey, TEXT, forceRequired);
    }

    protected final List<String> getTextList(String configKey) {
        return getTextList(configKey, false);
    }

    public final List<String> getTextList(String configKey, boolean forceRequired) {
        String value = get(configKey, TEXT_LIST, forceRequired);
        String[] elements = new String[0];
        if (value != null) {
            elements = value.split(",");
            if (elements.length == 1 && elements[0].isBlank()) {
                return Collections.emptyList();
            }
        }
        return Arrays.asList(elements);
    }

    public final String getBooleanStr(String configKey) {
        return get(configKey, BOOL, false);
    }

    public final boolean getBoolean(String configKey) {
        return get(configKey, BOOL, false).equals(Boolean.TRUE.toString());
    }

    public final String getHost(String configKey) {
        return get(configKey, HOST, false);
    }

    public final String getHost(String configKey, boolean forceRequired) {
        String value = get(configKey, HOST, false);
        if (forceRequired && value == null) {
            throw new ConfigException("Missing required parameter [%s]".formatted(configKey));
        }
        return value;
    }

    public final String getUrl(String configKey, boolean forceRequired) {
        return get(configKey, URL, forceRequired);
    }

    public final String getHostsList(String configKey) {
        return get(configKey, ConfType.HOST_LIST, false);
    }

    public final String getDirectory(String configKey) {
        return get(configKey, ConfType.DIRECTORY, false);
    }

    public final String getFile(String configKey) {
        return getFile(configKey, false);
    }

    public final String getFile(String configKey, boolean forceRequired) {
        return get(configKey, ConfType.FILE, forceRequired);
    }

    protected final String get(String key, Type type, boolean forceRequired) {
        ConfParameter param = configSpec.getParameter(key);
        if (param != null) {
            if (param.type().equals(type)) {
                if (param.required() && configuration.containsKey(key)) {
                    return configuration.get(key);
                } else {
                    String value =
                            configuration.getOrDefault(
                                    key, param.defaultHolder().value(configuration));
                    if (forceRequired && value == null) {
                        throw new ConfigException("Missing required parameter [%s]".formatted(key));
                    }
                    return value;
                }
            }
        }
        throw new ConfigException(
                "No parameter [%s] of %s type is present in the configuration"
                        .formatted(key, type));
    }

    public final Map<String, String> getValues(String configKey, boolean remap) {
        ConfParameter param = configSpec.getParameter(configKey);
        if (param.multiple()) {
            Map<String, String> newMap = new HashMap<>();
            for (Map.Entry<String, String> e : configuration.entrySet()) {
                if (remap) {
                    Optional<String> infix = ConfigsSpec.extractInfix(param, e.getKey());
                    if (infix.isPresent()) {
                        newMap.put(infix.get(), e.getValue());
                    }
                } else {
                    if (e.getKey().startsWith(configKey)) {
                        newMap.put(e.getKey(), e.getValue());
                    }
                }
            }
            return newMap;
        }
        return Collections.emptyMap();
    }

    public final <T> List<T> getAsList(
            String configKey, Function<? super Entry<String, String>, T> conv) {
        Map<String, String> values = getValues(configKey, true);
        return values.entrySet().stream().map(conv).toList();
    }

    public static final Map<String, String> appendAdapterDir(
            ConfigsSpec configSpec, Map<String, String> config, File configDir) {
        Map<String, String> updatedConfigs = new HashMap<>(config);
        updatedConfigs.put(ADAPTER_DIR, configDir.getAbsolutePath());
        List<ConfParameter> confParams = configSpec.getByType(FILE);
        for (ConfParameter confParameter : confParams) {
            String value = updatedConfigs.get(confParameter.name());
            if (value != null && !value.isBlank()) {
                updatedConfigs.replace(
                        confParameter.name(),
                        Paths.get(updatedConfigs.get(ADAPTER_DIR), value).toString());
            }
        }
        return updatedConfigs;
    }
}
