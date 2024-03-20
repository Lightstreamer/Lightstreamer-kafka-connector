
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

import com.lightstreamer.kafka_connector.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka_connector.adapters.config.nested.AuthenticationConfigs;
import com.lightstreamer.kafka_connector.adapters.config.nested.GssapiConfigs;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;

import org.apache.kafka.common.config.SaslConfigs;

import scala.collection.mutable.StringBuilder;

import java.util.Objects;
import java.util.Properties;

public class BrokerAuthenticationConfigs {

    public static final String NAME_SPACE = "authentication";
    public static final String SASL_MECHANISM = adapt(AuthenticationConfigs.SASL_MECHANISM);

    public static final String USERNAME = adapt(AuthenticationConfigs.USERNAME);
    public static final String PASSWORD = adapt(AuthenticationConfigs.PASSWORD);

    public static final String GSSAPI_KERBEROS_SERVICE_NAME =
            adapt(GssapiConfigs.GSSAPI_KERBEROS_SERVICE_NAME);
    public static final String GSSAPI_KEY_TAB_ENABLE = adapt(GssapiConfigs.GSSAPI_KEY_TAB_ENABLE);
    public static final String GSSAPI_KEY_TAB_PATH = adapt(GssapiConfigs.GSSAPI_KEY_TAB_PATH);
    public static final String GSSAPI_STORE_KEY_ENABLE =
            adapt(GssapiConfigs.GSSAPI_STORE_KEY_ENABLE);
    public static final String GSSAPI_PRINCIPAL = adapt(GssapiConfigs.GSSAPI_PRINCIPAL);
    public static final String GSSAPI_TICKET_CACHE_ENABLE =
            adapt(GssapiConfigs.GSSAPI_TICKET_CACHE_ENABLE);

    private static ConfigsSpec CONFIG_SPEC =
            AuthenticationConfigs.cloneSpec()
                    .withEnabledChildConfigs(
                            GssapiConfigs.spec(),
                            (map, key) -> SaslMechanism.GSSAPI.toString().equals(map.get(key)),
                            AuthenticationConfigs.SASL_MECHANISM)
                    .newSpecWithNameSpace(NAME_SPACE);

    static String adapt(String key) {
        return NAME_SPACE + "." + key;
    }

    static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    public static void withAuthenticationConfigs(ConfigsSpec config, String enablingKey) {
        config.withEnabledChildConfigs(CONFIG_SPEC, enablingKey);
    }

    static Properties addAuthentication(ConnectorConfig config) {
        NonNullKeyProperties props = new NonNullKeyProperties();
        if (config.isAuthenticationEnabled()) {
            SaslMechanism mechanism = config.authenticationMechanism();
            props.setProperty(SaslConfigs.SASL_MECHANISM, mechanism.toProperty());
            props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, configuredWith(config));
            if (config.isGssapiEnabled()) {
                props.setProperty(
                        SaslConfigs.SASL_KERBEROS_SERVICE_NAME, config.gssapiKerberosServiceName());
            }
        }
        return props.properties();
    }

    static String configuredWith(ConnectorConfig config) {
        if (config.isGssapiEnabled()) {
            return new GssapiJaas()
                    .useKeyTab(config.gssapiUseKeyTab())
                    .storeKey(config.gssapiStoreKey())
                    .keyTab(config.gssapiKeyTab())
                    .principal(config.gssapiPrincipal())
                    .useTicketCache(config.gssapiUseTicketCache())
                    .build();
        }
        return new Jaas()
                .withMechanism(config.authenticationMechanism())
                .withUsername(config.authenticationUsername())
                .withPassword(config.authenticationPassword())
                .build();
    }

    private static class GssapiJaas {

        private boolean useKeyTab;

        private boolean storeKey;

        private String keyTab;

        private String principal;

        private boolean useTicketCache;

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

        GssapiJaas useTicketCache(boolean useTicketCache) {
            this.useTicketCache = useTicketCache;
            return this;
        }

        public String build() {
            StringBuilder sb = new StringBuilder(SaslMechanism.GSSAPI.loginModule());
            sb.append(" required");
            if (useTicketCache) {
                sb.append(" useTicketCache=%s".formatted(useTicketCache));
            } else {
                sb.append(" useKeyTab=%s storeKey=%s".formatted(useKeyTab, storeKey));
                if (keyTab != null) {
                    sb.append(" keyTab='%s'".formatted(keyTab));
                }
                sb.append(" principal='%s'".formatted(principal));
            }
            sb.append(";");
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
