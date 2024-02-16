
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
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType;

import org.apache.kafka.common.config.SaslConfigs;

import scala.collection.mutable.StringBuilder;

import java.util.Objects;
import java.util.Properties;

public class AuthenticationConfigs {

    public static final String SASL_MECHANISM = "authentication.mechanism";

    public static final String USERNAME = "authentication.username";
    public static final String PASSWORD = "authentication.password";

    public static final String GSSAPI_USE_KEY_TAB = "authentication.gssapi.use.key.tab";
    public static final String GSSAPI_STORE_KEY = "authentication.gssapi.store.key";
    public static final String GSSAPI_KEY_TAB = "authentication.gssapi.key.tab";
    public static final String GSSAPI_KERBEROS_SERVICE_NAME =
            "authentication.gssapi.kerberos.service.name";
    public static final String GSSAPI_PRINCIPAL = "authentication.gssapi.principal";

    private static ConfigsSpec CONFIG_SPEC;

    private static ConfigsSpec GSSAPI_CONFIG_SEPC;

    static {
        GSSAPI_CONFIG_SEPC =
                new ConfigsSpec("authentication.gssapi")
                        .add(GSSAPI_USE_KEY_TAB, false, false, BOOL, defaultValue("false"))
                        .add(GSSAPI_STORE_KEY, false, false, BOOL, defaultValue("false"))
                        .add(GSSAPI_KEY_TAB, false, false, FILE)
                        .add(GSSAPI_PRINCIPAL, true, false, TEXT)
                        .add(GSSAPI_KERBEROS_SERVICE_NAME, true, false, TEXT);

        CONFIG_SPEC =
                new ConfigsSpec("authentication")
                        .add(
                                SASL_MECHANISM,
                                false,
                                false,
                                ConfType.SASL_MECHANISM,
                                defaultValue(SaslMechanism.PLAIN.toString()))
                        .add(USERNAME, false, false, TEXT)
                        .add(PASSWORD, false, false, TEXT)
                        // GSSAPI configuration enabled only if key "authentication.mechanism" is
                        // set to "GSSAPI"
                        .addChildConfigs(
                                GSSAPI_CONFIG_SEPC,
                                SASL_MECHANISM,
                                (map, key) -> SaslMechanism.GSSAPI.toString().equals(map.get(key)));
    }

    static ConfigsSpec configSpec() {
        return CONFIG_SPEC;
    }

    static ConfigsSpec gssapiConfigSpec() {
        return GSSAPI_CONFIG_SEPC;
    }

    public static void withAuthenticationConfigs(ConfigsSpec config, String enablingKey) {
        config.addChildConfigs(CONFIG_SPEC, enablingKey);
    }

    static Properties addAuthentication(ConnectorConfig config) {
        Properties props = new Properties();
        if (config.isAuthenticationEnabled()) {
            SaslMechanism mechanism = config.getAuthenticationMechanism();
            props.setProperty(SaslConfigs.SASL_MECHANISM, mechanism.toString());
            props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, configuredWith(config));
            if (config.isGssapiEnabled()) {
                props.setProperty(
                        SaslConfigs.SASL_KERBEROS_SERVICE_NAME, config.gssapiKerberosServiceName());
            }
        }
        return props;
    }

    static String configuredWith(ConnectorConfig config) {
        if (config.isGssapiEnabled()) {
            return new GssapiJaas()
                    .useKeyTab(config.gssapiUseKeyTab())
                    .storeKey(config.gssapiStoreKey())
                    .keyTab(config.gssapiKeyTab())
                    .principal(config.gssapiPrincipal())
                    .build();
        }
        return new Jaas()
                .withMechanism(config.getAuthenticationMechanism())
                .withUsername(config.getAuthenticationUsername())
                .withPassword(config.getAuthenticationPassword())
                .build();
    }

    private static class GssapiJaas {

        private boolean useKeyTab;

        private boolean storeKey;

        private String keyTab;

        private String principal;

        GssapiJaas useKeyTab(boolean useKeyTab) {
            this.useKeyTab = useKeyTab;
            return this;
        }

        GssapiJaas storeKey(boolean storeKey) {
            this.storeKey = storeKey;
            return this;
        }

        GssapiJaas keyTab(String keyTab) {
            this.keyTab = keyTab;
            return this;
        }

        GssapiJaas principal(String principal) {
            this.principal = principal;
            return this;
        }

        public String build() {
            StringBuilder sb = new StringBuilder(SaslMechanism.GSSAPI.loginModule());
            sb.append(" required");
            sb.append(" useKeyTab=").append(String.valueOf(useKeyTab));
            sb.append(" storeKey=").append(String.valueOf(storeKey));
            if (keyTab != null) {
                sb.append(" keyTab='" + keyTab + "'");
            }
            sb.append(" principal='" + principal + "';");
            return sb.toString();
        }
    }

    private static class Jaas {

        private String username;

        private String password;

        private SaslMechanism mechanism;

        public Jaas withMechanism(SaslMechanism mechanism) {
            this.mechanism = mechanism;
            return this;
        }

        public Jaas withUsername(String username) {
            this.username = username;
            return this;
        }

        public Jaas withPassword(String password) {
            this.password = password;
            return this;
        }

        public String build() {
            Objects.requireNonNull(username);
            Objects.requireNonNull(password);
            Objects.requireNonNull(mechanism);
            return String.format(
                    "%s required username='%s' password='%s';",
                    mechanism.loginModule(), username, password);
        }
    }

    public static void main(String[] args) {}
}
