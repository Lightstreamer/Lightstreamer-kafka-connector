
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

import com.lightstreamer.kafka.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka.adapters.config.nested.AuthenticationConfigs;
import com.lightstreamer.kafka.adapters.config.nested.AwsMskIamConfigs;
import com.lightstreamer.kafka.adapters.config.nested.GssapiConfigs;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;

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

    public static final String AWS_MSK_IAM_CREDENTIAL_PROFILE_NAME =
            adapt(AwsMskIamConfigs.CREDENTIAL_PROFILE_NAME);

    public static final String AWS_MSK_IAM_ROLE_ARN = adapt(AwsMskIamConfigs.ROLE_ARN);

    public static final String AWS_MSK_IAM_ROLE_SESSION_NAME =
            adapt(AwsMskIamConfigs.ROLE_SESSION_NAME);

    public static final String AWS_MSK_IAM_STS_REGION = adapt(AwsMskIamConfigs.STS_REGION);

    private static final String AWS_MSK_IAM_CALLBACK_HANDLER_CLASS =
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler";

    private static ConfigsSpec CONFIG_SPEC =
            AuthenticationConfigs.cloneSpec()
                    .withEnabledChildConfigs(
                            GssapiConfigs.spec(),
                            (map, key) -> SaslMechanism.GSSAPI.toString().equals(map.get(key)),
                            AuthenticationConfigs.SASL_MECHANISM)
                    .withEnabledChildConfigs(
                            AwsMskIamConfigs.spec(),
                            (map, key) -> SaslMechanism.AWS_MSK_IAM.toString().equals(map.get(key)),
                            AuthenticationConfigs.SASL_MECHANISM)
                    .newSpecWithNameSpace(NAME_SPACE);

    static String adapt(String key) {
        return NAME_SPACE + "." + key;
    }

    static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    static Properties addAuthentication(ConnectorConfig config) {
        NonNullKeyProperties props = new NonNullKeyProperties();
        if (config.isAuthenticationEnabled()) {
            SaslMechanism mechanism = config.authenticationMechanism();
            props.setProperty(SaslConfigs.SASL_MECHANISM, mechanism.toString());
            props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfiguredWith(config));
            if (config.isGssapiEnabled()) {
                props.setProperty(
                        SaslConfigs.SASL_KERBEROS_SERVICE_NAME, config.gssapiKerberosServiceName());
            }
            if (config.isAwsMskIamEnabled()) {
                props.setProperty(
                        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
                        AWS_MSK_IAM_CALLBACK_HANDLER_CLASS);
            }
        }
        return props.unmodifiable();
    }

    static String jaasConfiguredWith(ConnectorConfig config) {
        JaasConfig jaasConfig =
                switch (config.authenticationMechanism()) {
                    case PLAIN, SCRAM_256, SCRAM_512 ->
                            new Jaas(config.authenticationMechanism())
                                    .withUsername(config.authenticationUsername())
                                    .withPassword(config.authenticationPassword());
                    case GSSAPI ->
                            new GssapiJaas()
                                    .useKeyTab(config.gssapiUseKeyTab())
                                    .storeKey(config.gssapiStoreKey())
                                    .keyTab(config.gssapiKeyTab())
                                    .principal(config.gssapiPrincipal())
                                    .useTicketCache(config.gssapiUseTicketCache());
                    case AWS_MSK_IAM ->
                            new AwsMskIamJaas()
                                    .withCredentialProfileName(
                                            config.awsMskIamCredentialProfileName())
                                    .withRoleArn(config.awsMskIamRoleArn())
                                    .withRoleSessionName(config.awsMskIamRoleSessionName())
                                    .withStsRegion(config.awsMskIamStsRegion());
                };
        return jaasConfig.build();
    }

    private abstract static class JaasConfig {

        private final SaslMechanism mechanism;

        JaasConfig(SaslMechanism mechanism) {
            this.mechanism = mechanism;
        }

        public String build() {
            StringBuilder sb = new StringBuilder(mechanism.loginModule());
            sb.append(" required");
            doBuild(sb);
            sb.append(";");
            return sb.toString();
        }

        protected abstract void doBuild(StringBuilder sb);
    }

    private static class Jaas extends JaasConfig {

        private String username;

        private String password;

        public Jaas(SaslMechanism mechanism) {
            super(mechanism);
        }

        public Jaas withUsername(String username) {
            this.username = username;
            return this;
        }

        public Jaas withPassword(String password) {
            this.password = password;
            return this;
        }

        protected void doBuild(StringBuilder sb) {
            Objects.requireNonNull(username);
            Objects.requireNonNull(password);
            sb.append(String.format(" username='%s' password='%s'", username, password));
        }
    }

    private static class GssapiJaas extends JaasConfig {

        GssapiJaas() {
            super(SaslMechanism.GSSAPI);
        }

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

        protected void doBuild(StringBuilder sb) {
            if (useTicketCache) {
                sb.append(" useTicketCache=%s".formatted(useTicketCache));
            } else {
                sb.append(" useKeyTab=%s storeKey=%s".formatted(useKeyTab, storeKey));
                if (keyTab != null) {
                    sb.append(" keyTab='%s'".formatted(keyTab));
                }
                sb.append(" principal='%s'".formatted(principal));
            }
        }
    }

    public static class AwsMskIamJaas extends JaasConfig {

        private String credentialProfileName;
        private String roleArn;
        private String roleSessionName;
        private String stsRegion;

        AwsMskIamJaas() {
            super(SaslMechanism.AWS_MSK_IAM);
        }

        public AwsMskIamJaas withCredentialProfileName(String credentialProfileName) {
            this.credentialProfileName = credentialProfileName;
            return this;
        }

        public AwsMskIamJaas withRoleArn(String roleArn) {
            this.roleArn = roleArn;
            return this;
        }

        public AwsMskIamJaas withRoleSessionName(String roleSessionName) {
            this.roleSessionName = roleSessionName;
            return this;
        }

        public AwsMskIamJaas withStsRegion(String stsRegion) {
            this.stsRegion = stsRegion;
            return this;
        }

        protected void doBuild(StringBuilder sb) {
            if (credentialProfileName != null) {
                sb.append(String.format(" awsProfileName=\"%s\"".formatted(credentialProfileName)));
            } else if (roleArn != null) {
                sb.append(String.format(" awsRoleArn=\"%s\"", roleArn));
                if (roleSessionName != null) {
                    sb.append(String.format(" awsRoleSessionName=\"%s\"", roleSessionName));
                }
                if (stsRegion != null) {
                    sb.append(String.format(" awsStsRegion=\"%s\"", stsRegion));
                }
            }
        }
    }
}
