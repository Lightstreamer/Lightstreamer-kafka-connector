
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
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SaslMechanism;

import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

public class AuthenticationConfigSpec {

    public static final String SASL_MECHANISM = "authentication.mechanism";

    public static final String USERNAME = "authentication.username";

    public static final String PASSWORD = "authentication.password";

    private static ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigSpec("authentication")
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

    static Properties addAuthentication(ConnectorConfig config) {
        Properties properties = new Properties();
        if (config.isAuthenticationEnabled()) {
            properties.setProperty(
                    SaslConfigs.SASL_MECHANISM, config.getAuthenticationMechanismStr());
            properties.setProperty(
                    SaslConfigs.SASL_JAAS_CONFIG, JaasConfig.fromConnectorConfig(config));
        }
        return properties;
    }

    private static class JaasConfig {

        private String username;

        private String password;

        private SaslMechanism mechanism;

        static String fromConnectorConfig(ConnectorConfig config) {
            return new JaasConfig()
                    .withMechanism(config.getAuthenticationMechanism())
                    .withUsername(config.getAuthenticationUsername())
                    .withPassword(config.getAuthenticationPassword())
                    .build();
        }

        public JaasConfig withMechanism(SaslMechanism mechanism) {
            this.mechanism = mechanism;
            return this;
        }

        public JaasConfig withUsername(String username) {
            this.username = username;
            return this;
        }

        public JaasConfig withPassword(String password) {
            this.password = password;
            return this;
        }

        public String build() {
            return String.format(
                    "%s required username='%s' password='%s';",
                    mechanism.loginModule(), username, password);
        }
    }
}
