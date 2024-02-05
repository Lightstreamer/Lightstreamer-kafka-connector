
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

import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.FILE;

import java.util.Map;

public final class GlobalConfig extends AbstractConfig {

    public static final String LOGGING_CONFIGURATION_FILE = "logging.configuration.file";

    private static final ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec().add(LOGGING_CONFIGURATION_FILE, true, false, FILE);
    }

    private GlobalConfig(ConfigSpec spec, Map<String, String> configs) {
        super(spec, configs);
    }

    public GlobalConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);
    }

    static ConfigSpec configSpec() {
        return CONFIG_SPEC;
    }
}
