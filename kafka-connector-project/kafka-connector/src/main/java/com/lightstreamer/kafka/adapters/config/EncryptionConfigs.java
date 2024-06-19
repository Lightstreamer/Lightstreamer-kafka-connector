
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
import com.lightstreamer.kafka.adapters.config.nested.KeystoreConfigs;
import com.lightstreamer.kafka.adapters.config.nested.TlsConfigs;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SecurityProtocol;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class EncryptionConfigs {

    public static final String NAME_SPACE = "encryption";

    public static final String SSL_PROTOCOL = adapt(TlsConfigs.SSL_PROTOCOL);
    public static final String SSL_ENABLED_PROTOCOLS = adapt(TlsConfigs.SSL_ENABLED_PROTOCOLS);
    public static final String SSL_CIPHER_SUITES = adapt(TlsConfigs.SSL_CIPHER_SUITES);

    public static final String TRUSTSTORE_TYPE = adapt(TlsConfigs.TRUSTSTORE_TYPE);
    public static final String TRUSTSTORE_PATH = adapt(TlsConfigs.TRUSTSTORE_PATH);
    public static final String TRUSTSTORE_PASSWORD = adapt(TlsConfigs.TRUSTSTORE_PASSWORD);

    public static final String ENABLE_MTLS = adapt(TlsConfigs.KESYTORE_ENABLE);

    public static final String ENABLE_HOSTNAME_VERIFICATION =
            adapt(TlsConfigs.HOSTNAME_VERIFICATION_ENABLE);

    public static final String SSL_PROVIDER = adapt(TlsConfigs.SSL_PROVIDER);
    public static final String SSL_EGINE_FACTORY_CLASS = adapt(TlsConfigs.SSL_EGINE_FACTORY_CLASS);
    public static final String SSL_KEYMANAGER_ALGORITHM =
            adapt(TlsConfigs.SSL_KEYMANAGER_ALGORITHM);
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION =
            adapt(TlsConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION);
    public static final String SSL_TRUSTMANAGER_ALGORITHM =
            adapt(TlsConfigs.SSL_TRUSTMANAGER_ALGORITHM);
    public static final String SECURITY_PROVIDERS = adapt(TlsConfigs.SECURITY_PROVIDERS);

    public static final String KEYSTORE_TYPE = adapt(KeystoreConfigs.KEYSTORE_TYPE);
    public static final String KEYSTORE_PATH = adapt(KeystoreConfigs.KEYSTORE_PATH);
    public static final String KEYSTORE_PASSWORD = adapt(KeystoreConfigs.KEYSTORE_PASSWORD);
    public static final String KEY_PASSWORD = adapt(KeystoreConfigs.KEY_PASSWORD);

    private static ConfigsSpec CONFIG_SPEC = TlsConfigs.spec().newSpecWithNameSpace(NAME_SPACE);

    static String adapt(String key) {
        return NAME_SPACE + "." + key;
    }

    static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    static Properties addEncryption(ConnectorConfig cfg) {
        NonNullKeyProperties props = new NonNullKeyProperties();
        SecurityProtocol protocol =
                SecurityProtocol.retrieve(cfg.isEncryptionEnabled(), cfg.isAuthenticationEnabled());
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());
        if (cfg.isEncryptionEnabled()) {
            props.setProperty(SslConfigs.SSL_PROTOCOL_CONFIG, cfg.sslProtocol().toString());
            props.setProperty(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, cfg.enabledProtocolsAsStr());
            props.setProperty(
                    SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, cfg.truststoreType().toString());
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, cfg.truststorePath());
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, cfg.truststorePassword());
            if (!cfg.isHostNameVerificationEnabled()) {
                props.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }
            props.setProperty(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cfg.cipherSuitesAsStr());
            props.setProperty(SslConfigs.SSL_PROVIDER_CONFIG, cfg.sslProvider());
            props.setProperty(
                    SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG,
                    cfg.getText(SSL_EGINE_FACTORY_CLASS));
            props.setProperty(
                    SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
                    cfg.getText(SSL_SECURE_RANDOM_IMPLEMENTATION));
            props.setProperty(
                    SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                    cfg.getText(SSL_TRUSTMANAGER_ALGORITHM));
            props.setProperty(
                    SecurityConfig.SECURITY_PROVIDERS_CONFIG, cfg.getText(SECURITY_PROVIDERS));

            if (cfg.isKeystoreEnabled()) {
                props.setProperty(
                        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, cfg.keystoreType().toString());
                props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, cfg.keystorePassword());
                props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, cfg.keystorePath());
                props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, cfg.keyPassword());
            }
        }
        return props.properties();
    }
}
