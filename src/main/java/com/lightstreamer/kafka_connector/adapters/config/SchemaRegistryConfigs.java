
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
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka_connector.adapters.config.nested.KeystoreConfigs;
import com.lightstreamer.kafka_connector.adapters.config.nested.TlsConfigs;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class SchemaRegistryConfigs {

    public static final String NAME_SPACE = "schema.registry";
    public static final String URL = ns("url");

    public static final String ENCRYPTION_NAME_SPACE = ns("encryption");
    public static final String SSL_ENABLED_PROTOCOLS = nse(TlsConfigs.SSL_ENABLED_PROTOCOLS);
    public static final String SSL_PROTOCOL = nse(TlsConfigs.SSL_PROTOCOL);

    public static final String TRUSTSTORE_TYPE = nse(TlsConfigs.TRUSTSTORE_TYPE);
    public static final String TRUSTSTORE_PATH = nse(TlsConfigs.TRUSTSTORE_PATH);
    public static final String TRUSTSTORE_PASSWORD = nse(TlsConfigs.TRUSTSTORE_PASSWORD);

    public static final String ENABLE_MTLS = nse(TlsConfigs.ENABLE_KESYTORE);

    public static final String ENABLE_HOSTNAME_VERIFICATION =
            nse(TlsConfigs.ENABLE_HOSTNAME_VERIFICATION);

    public static final String SSL_CIPHER_SUITES = nse(TlsConfigs.SSL_CIPHER_SUITES);
    public static final String SSL_PROVIDER = nse(TlsConfigs.SSL_PROVIDER);
    public static final String SSL_EGINE_FACTORY_CLASS = nse(TlsConfigs.SSL_EGINE_FACTORY_CLASS);
    public static final String SSL_KEYMANAGER_ALGORITHM = nse(TlsConfigs.SSL_KEYMANAGER_ALGORITHM);
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION =
            nse(TlsConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION);
    public static final String SSL_TRUSTMANAGER_ALGORITHM =
            nse(TlsConfigs.SSL_TRUSTMANAGER_ALGORITHM);
    public static final String SECURITY_PROVIDERS = nse(TlsConfigs.SECURITY_PROVIDERS);

    public static final String KEYSTORE_TYPE = nse(KeystoreConfigs.KEYSTORE_TYPE);
    public static final String KEYSTORE_PATH = nse(KeystoreConfigs.KEYSTORE_PATH);
    public static final String KEYSTORE_PASSWORD = nse(KeystoreConfigs.KEYSTORE_PASSWORD);
    public static final String KEY_PASSWORD = nse(KeystoreConfigs.KEY_PASSWORD);

    public static final String ENABLE_BASIC_AUTHENTICATION = ns("basic.authentication.enabled");
    public static final String BASIC_AUTHENTICATION_USER_NAME = ns("basic.authentication.username");
    public static final String BASIC_AUTHENTICATION_USER_PASSWORD =
            ns("basic.authentication.passowrd");

    private static ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("schemaRegistry")
                        .add(URL, true, false, ConfType.URL)
                        .add(ENABLE_BASIC_AUTHENTICATION, false, false, BOOL, defaultValue("false"))
                        .withEnabledChildConfigs(
                                new ConfigsSpec("basicAuthentication")
                                        .add(BASIC_AUTHENTICATION_USER_NAME, true, false, TEXT)
                                        .add(BASIC_AUTHENTICATION_USER_PASSWORD, true, false, TEXT),
                                ENABLE_BASIC_AUTHENTICATION)
                        .withEnabledChildConfigs(
                                TlsConfigs.spec().newSpecWithNameSpace(ENCRYPTION_NAME_SPACE),
                                (map, key) -> map.get(key).startsWith("https"),
                                URL);
    }

    static String ns(String key) {
        return NAME_SPACE + "." + key;
    }

    static String nse(String key) {
        return ENCRYPTION_NAME_SPACE + "." + key;
    }

    static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    static Properties addSchemaRegistry(ConnectorConfig cfg) {
        NonNullKeyProperties props = new NonNullKeyProperties();
        if (!cfg.isSchemaRegistryEnabled()) {
            return props.properties();
        }

        props.setProperty(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cfg.schemaRegistryUrl());

        if (cfg.getKeyEvaluator().equals(EvaluatorType.JSON)) {
            props.setProperty(
                    KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, JsonNode.class.getName());
        }
        if (cfg.getValueEvaluator().equals(EvaluatorType.JSON)) {
            props.setProperty(
                    KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
        }

        if (cfg.isSchemaRegistryEncryptionEnabled()) {
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_PROTOCOL_CONFIG,
                    cfg.schemaRegistrySslProtocol().toString());
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
                    cfg.schemaRegistryEnabledProtocolsAsStr());
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                    cfg.schemaRegistryTruststoreType().toString());
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                    cfg.schemaRegistryTruststorePath());
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                    cfg.schemaRegistryTruststorePassword());
            if (!cfg.isSchemaRegistryHostNameVerificationEnabled()) {
                props.setProperty(
                        "schema.registry."
                                + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                        "");
            }
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_CIPHER_SUITES_CONFIG,
                    cfg.schemaRegistryCipherSuitesAsStr());
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_PROVIDER_CONFIG,
                    cfg.schemaRegistrySslProvider());
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG,
                    cfg.getText(SSL_EGINE_FACTORY_CLASS));
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
                    cfg.getText(SSL_SECURE_RANDOM_IMPLEMENTATION));
            props.setProperty(
                    "schema.registry." + SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                    cfg.getText(SSL_TRUSTMANAGER_ALGORITHM));
            props.setProperty(
                    "schema.registry." + SecurityConfig.SECURITY_PROVIDERS_CONFIG,
                    cfg.getText(SECURITY_PROVIDERS));

            if (cfg.isSchemaRegistryKeystoreEnabled()) {
                props.setProperty(
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
                        cfg.schemaRegistryKeystoreType().toString());
                props.setProperty(
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        cfg.schemaRegistryKeystorePassword());
                props.setProperty(
                        "schema.registry." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        cfg.schemaRegistryKeystorePath());
                props.setProperty(
                        "schema.registry." + SslConfigs.SSL_KEY_PASSWORD_CONFIG,
                        cfg.schemaRegistryKeyPassword());
            }
        }
        return props.properties();
    }
}
