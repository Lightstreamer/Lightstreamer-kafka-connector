
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

import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.BOOL;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.commons.NoNullKeyProperties;
import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SecurityProtocol;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SslProtocol;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class EncryptionConfigs {

    public static String SSL_ENABLED_PROTOCOLS = "encryption.enabled.protocols";
    public static String SSL_PROTOCOL = "encryption.protocol";

    public static String TRUSTSTORE_TYPE = "encryption.truststore.type";
    public static String TRUSTSTORE_PATH = "encryption.truststore.path";
    public static String TRUSTSTORE_PASSWORD = "encryption.truststore.password";

    public static String ENABLE_MTLS = "encryption.keystore.enabled";

    public static String ENABLE_HOSTNAME_VERIFICATION =
            "encryption.endpoint.identification.algorithm";
    public static String SSL_CIPHER_SUITES = "encryption.cipher.suites";
    public static String SSL_PROVIDER = "encryption.provider";
    public static String SSL_EGINE_FACTORY_CLASS = "encryption.engine.factory.class";
    public static String SSL_KEYMANAGER_ALGORITHM = "encryption.keymanager.algorithm";
    public static String SSL_SECURE_RANDOM_IMPLEMENTATION =
            "encryption.secure.random.implementation";
    public static String SSL_TRUSTMANAGER_ALGORITHM = "encryption.trustmanager.algorithm";
    public static String SECURITY_PROVIDERS = "encryption.security.providers";

    private static ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigSpec("encryption")
                        .add(
                                SSL_ENABLED_PROTOCOLS,
                                false,
                                false,
                                ConfType.SSL_ENABLED_PROTOCOLS,
                                defaultValue(ConfigTypes.SslProtocol.toValuesStr()))
                        .add(
                                SSL_PROTOCOL,
                                false,
                                false,
                                ConfType.SSL_PROTOCOL,
                                defaultValue(SslProtocol.TLSv13.toString()))
                        .add(
                                TRUSTSTORE_TYPE,
                                false,
                                false,
                                ConfType.KEYSTORE_TYPE,
                                defaultValue(KeystoreType.JKS.toString()))
                        .add(TRUSTSTORE_PATH, false, false, FILE)
                        .add(TRUSTSTORE_PASSWORD, false, false, TEXT)
                        .add(
                                ENABLE_HOSTNAME_VERIFICATION,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(SSL_CIPHER_SUITES, false, false, ConfType.TEXT_LIST)
                        .add(SSL_PROVIDER, false, false, TEXT)
                        .add(SSL_EGINE_FACTORY_CLASS, false, false, TEXT)
                        .add(SSL_KEYMANAGER_ALGORITHM, false, false, TEXT)
                        .add(SSL_SECURE_RANDOM_IMPLEMENTATION, false, false, TEXT)
                        .add(SSL_TRUSTMANAGER_ALGORITHM, false, false, TEXT)
                        .add(SECURITY_PROVIDERS, false, false, TEXT)
                        .add(ENABLE_MTLS, false, false, ConfType.BOOL, defaultValue("false"))
                        .withKeystoreConfigs(ENABLE_MTLS);
    }

    static ConfigSpec configSpec() {
        return CONFIG_SPEC;
    }

    static void withEncryptionConfigs(ConfigSpec config, String enablingKey) {
        config.addConfigSpec(CONFIG_SPEC, enablingKey);
    }

    static Properties addEncryption(ConnectorConfig cfg) {
        NoNullKeyProperties props = new NoNullKeyProperties();
        SecurityProtocol protocol =
                SecurityProtocol.retrieve(cfg.isEncryptionEnabled(), cfg.isAuthenticationEnabled());
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());
        if (cfg.isEncryptionEnabled()) {
            props.setProperty(
                    SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, cfg.getEnabledProtocolsAsStr());
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, cfg.getTrustStoreType());
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, cfg.getTrustStorePath());
            props.setProperty(
                    SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, cfg.getTrustStorePassword());
            if (!cfg.isHostNameVerificationEnabled()) {
                props.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }
            props.setProperty(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cfg.getCipherSuitesAsStr());
            props.setProperty(SslConfigs.SSL_PROVIDER_CONFIG, cfg.getSslProvider());
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
            props.putAll(KeystoreConfigs.addKeystore(cfg));
        }
        return props.properties();
    }
}
