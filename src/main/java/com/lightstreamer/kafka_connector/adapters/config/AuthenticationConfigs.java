
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

import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;

import java.util.Properties;

public class AuthenticationConfigs {

    public static final String SASL_MECHANISM = "authentication.mecanism";

    public static final String USERNAME = "authentication.username";

    public static final String PASSWORD = "authentication.password";

    private static ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigSpec()
                        .add(
                                SASL_MECHANISM,
                                false,
                                false,
                                ConfType.SASL_MECHANISM,
                                defaultValue(ConfigTypes.SaslMechanism.PLAIN.toString()))
                        .add(USERNAME, true, false, ConfType.TEXT)
                        .add(PASSWORD, true, false, ConfType.TEXT);
    }

    static ConfigSpec configSpec() {
        return CONFIG_SPEC;
    }

    static void withAuthenticationConfigs(ConfigSpec config, String enablingKey) {
        config.addConfigSpec(CONFIG_SPEC, enablingKey);
    }

    static Properties addEncryption(ConnectorConfig config) {
        Properties properties = new Properties();
        return properties;
    }
}
