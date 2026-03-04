
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka.adapters.config.nested.KeystoreConfigs;
import com.lightstreamer.kafka.adapters.config.nested.TlsConfigs;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SchemaRegistryProvider;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;

import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Map;
import java.util.Properties;

public class SchemaRegistryConfigs {

    public static final String NAME_SPACE = "schema.registry";

    public static final String CONFLUENT_NAME_SPACE = ns("confluent");
    public static final String AZURE_NAME_SPACE = ns("azure");

    public static final String URL = ns("url");
    public static final String SCHEMA_REGISTRY_PROVIDER = ns("provider");

    public static final SchemaRegistryProvider DEFAULT_SCHEMA_REGISTRY_PROVIDER =
            SchemaRegistryProvider.CONFLUENT;

    // Confluent specific configs
    public static final String CONFLUENT_URL = cns("url");
    public static final String ENCRYPTION_NAME_SPACE = cns("encryption");
    public static final String SSL_ENABLED_PROTOCOLS = cens(TlsConfigs.SSL_ENABLED_PROTOCOLS);
    public static final String SSL_PROTOCOL = cens(TlsConfigs.SSL_PROTOCOL);

    public static final String TRUSTSTORE_TYPE = cens(TlsConfigs.TRUSTSTORE_TYPE);
    public static final String TRUSTSTORE_PATH = cens(TlsConfigs.TRUSTSTORE_PATH);
    public static final String TRUSTSTORE_PASSWORD = cens(TlsConfigs.TRUSTSTORE_PASSWORD);

    public static final String KEYSTORE_ENABLE = cens(TlsConfigs.KEYSTORE_ENABLE);

    public static final String HOSTNAME_VERIFICATION_ENABLE =
            cens(TlsConfigs.HOSTNAME_VERIFICATION_ENABLE);

    public static final String SSL_CIPHER_SUITES = cens(TlsConfigs.SSL_CIPHER_SUITES);
    public static final String SSL_PROVIDER = cens(TlsConfigs.SSL_PROVIDER);
    public static final String SSL_ENGINE_FACTORY_CLASS = cens(TlsConfigs.SSL_ENGINE_FACTORY_CLASS);
    public static final String SSL_KEYMANAGER_ALGORITHM = cens(TlsConfigs.SSL_KEYMANAGER_ALGORITHM);
    public static final String SSL_SECURE_RANDOM_IMPLEMENTATION =
            cens(TlsConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION);
    public static final String SSL_TRUSTMANAGER_ALGORITHM =
            cens(TlsConfigs.SSL_TRUSTMANAGER_ALGORITHM);
    public static final String SECURITY_PROVIDERS = cens(TlsConfigs.SECURITY_PROVIDERS);

    public static final String KEYSTORE_TYPE = cens(KeystoreConfigs.KEYSTORE_TYPE);
    public static final String KEYSTORE_PATH = cens(KeystoreConfigs.KEYSTORE_PATH);
    public static final String KEYSTORE_PASSWORD = cens(KeystoreConfigs.KEYSTORE_PASSWORD);
    public static final String KEY_PASSWORD = cens(KeystoreConfigs.KEY_PASSWORD);

    public static final String ENABLE_BASIC_AUTHENTICATION = cns("basic.authentication.enable");
    public static final String BASIC_AUTHENTICATION_USER_NAME =
            cns("basic.authentication.username");
    public static final String BASIC_AUTHENTICATION_USER_PASSWORD =
            cns("basic.authentication.password");

    // Azure specific configs
    public static final String AZURE_TENANT_ID = ans("azure.tenant.id");
    public static final String AZURE_CLIENT_ID = ans("azure.client.id");
    public static final String AZURE_CLIENT_SECRET = ans("azure.client.secret");

    private static ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("schemaRegistry")
                        .add(URL, true, false, ConfType.URL)
                        .add(
                                SCHEMA_REGISTRY_PROVIDER,
                                false,
                                false,
                                TEXT,
                                defaultValue(DEFAULT_SCHEMA_REGISTRY_PROVIDER.toString()))
                        .withEnabledChildConfigs(
                                new ConfigsSpec("confluent")
                                        .add(CONFLUENT_URL, true, false, ConfType.URL)
                                        .add(
                                                ENABLE_BASIC_AUTHENTICATION,
                                                false,
                                                false,
                                                BOOL,
                                                defaultValue("false"))
                                        .withEnabledChildConfigs(
                                                new ConfigsSpec("basicAuthentication")
                                                        .add(
                                                                BASIC_AUTHENTICATION_USER_NAME,
                                                                true,
                                                                false,
                                                                TEXT)
                                                        .add(
                                                                BASIC_AUTHENTICATION_USER_PASSWORD,
                                                                true,
                                                                false,
                                                                TEXT),
                                                ENABLE_BASIC_AUTHENTICATION)
                                        .withEnabledChildConfigs(
                                                TlsConfigs.spec()
                                                        .newSpecWithNameSpace(
                                                                ENCRYPTION_NAME_SPACE),
                                                (map, key) -> map.get(key).startsWith("https"),
                                                CONFLUENT_URL),
                                (map, key) ->
                                        SchemaRegistryProvider.CONFLUENT.equals(
                                                schemaRegistryProvider(map, key)),
                                SCHEMA_REGISTRY_PROVIDER)
                        .withEnabledChildConfigs(
                                new ConfigsSpec("azureCredentials")
                                        .add(AZURE_TENANT_ID, true, false, TEXT)
                                        .add(AZURE_CLIENT_ID, true, false, TEXT)
                                        .add(AZURE_CLIENT_SECRET, true, false, TEXT),
                                (map, key) ->
                                        SchemaRegistryProvider.AZURE.equals(
                                                schemaRegistryProvider(map, key)),
                                SCHEMA_REGISTRY_PROVIDER);
    }

    static SchemaRegistryProvider schemaRegistryProvider(Map<String, String> configs, String key) {
        return SchemaRegistryProvider.valueOf(
                configs.getOrDefault(key, DEFAULT_SCHEMA_REGISTRY_PROVIDER.toString()));
    }

    static String ns(String key) {
        return NAME_SPACE + "." + key;
    }

    static String cens(String key) {
        return ENCRYPTION_NAME_SPACE + "." + key;
    }

    static String cns(String key) {
        return CONFLUENT_NAME_SPACE + "." + key;
    }

    static String ans(String key) {
        return "azureCredentials." + key;
    }

    static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    static Properties addSchemaRegistry(ConnectorConfig cfg) {
        NonNullKeyProperties props = new NonNullKeyProperties();
        if (cfg.getKeyEvaluator().equals(EvaluatorType.JSON)) {
            props.setProperty(
                    KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, JsonNode.class.getName());
        }
        if (cfg.getValueEvaluator().equals(EvaluatorType.JSON)) {
            props.setProperty(
                    KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
        }

        if (!cfg.isSchemaRegistryEnabled()) {
            return props.unmodifiable();
        }

        props.setProperty(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cfg.schemaRegistryUrl());
        SchemaRegistryProvider provider = cfg.schemaRegistryProvider();
        props.setProperty(SCHEMA_REGISTRY_PROVIDER, provider.toString());

        switch (provider) {
            case AZURE -> {
                props.setProperty(AZURE_TENANT_ID, cfg.getText(AZURE_TENANT_ID));
                props.setProperty(AZURE_CLIENT_ID, cfg.getText(AZURE_CLIENT_ID));
                props.setProperty(AZURE_CLIENT_SECRET, cfg.getText(AZURE_CLIENT_SECRET));
            }

            case CONFLUENT -> {
                if (cfg.isSchemaRegistryEncryptionEnabled()) {
                    props.setProperty(
                            ns(SslConfigs.SSL_PROTOCOL_CONFIG),
                            cfg.schemaRegistrySslProtocol().toString());
                    props.setProperty(
                            ns(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG),
                            cfg.schemaRegistryEnabledProtocolsAsStr());
                    props.setProperty(
                            ns(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                            cfg.schemaRegistryTruststoreType().toString());
                    props.setProperty(
                            ns(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                            cfg.schemaRegistryTruststorePath());
                    props.setProperty(
                            ns(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
                            cfg.schemaRegistryTruststorePassword());
                    if (!cfg.isSchemaRegistryHostNameVerificationEnabled()) {
                        props.setProperty(
                                ns(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG), "");
                    }
                    props.setProperty(
                            ns(SslConfigs.SSL_CIPHER_SUITES_CONFIG),
                            cfg.schemaRegistryCipherSuitesAsStr());
                    props.setProperty(
                            ns(SslConfigs.SSL_PROVIDER_CONFIG), cfg.schemaRegistrySslProvider());
                    props.setProperty(
                            ns(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG),
                            cfg.getText(SSL_ENGINE_FACTORY_CLASS));
                    props.setProperty(
                            ns(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG),
                            cfg.getText(SSL_SECURE_RANDOM_IMPLEMENTATION));
                    props.setProperty(
                            ns(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG),
                            cfg.getText(SSL_TRUSTMANAGER_ALGORITHM));
                    props.setProperty(
                            ns(SecurityConfig.SECURITY_PROVIDERS_CONFIG),
                            cfg.getText(SECURITY_PROVIDERS));

                    if (cfg.isSchemaRegistryKeystoreEnabled()) {
                        props.setProperty(
                                ns(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                                cfg.schemaRegistryKeystoreType().toString());
                        props.setProperty(
                                ns(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                                cfg.schemaRegistryKeystorePassword());
                        props.setProperty(
                                ns(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                                cfg.schemaRegistryKeystorePath());
                        props.setProperty(
                                ns(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
                                cfg.schemaRegistryKeyPassword());
                    }
                }

                if (cfg.isSchemaRegistryBasicAuthenticationEnabled()) {
                    props.setProperty(
                            SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
                    props.setProperty(
                            SchemaRegistryClientConfig.USER_INFO_CONFIG,
                            "%s:%s"
                                    .formatted(
                                            cfg.schemaRegistryBasicAuthenticationUserName(),
                                            cfg.schemaRegistryBasicAuthenticationPassword()));
                }
            }
        }

        return props.unmodifiable();
    }

    private SchemaRegistryConfigs() {}
}
