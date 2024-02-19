
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
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.ERROR_STRATEGY;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.INT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.URL;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;

import com.lightstreamer.kafka_connector.adapters.commons.NoNullKeyProperties;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.SslProtocol;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType;

import org.apache.kafka.clients.admin.AdminClientConfig;

import java.io.File;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public final class ConnectorConfig extends AbstractConfig {

    public static final String ENABLED = "enabled";

    public static final String DATA_ADAPTER_NAME = "data_provider.name";

    public static final String GROUP_ID = "group.id";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String ITEM_TEMPLATE = "item-template";

    public static final String TOPIC_MAPPING = "map";
    private static final String MAP_SUFFIX = "to";

    public static final String FIELD_MAPPING = "field";

    public static final String KEY_EVALUATOR_TYPE = "key.evaluator.type";

    public static final String KEY_EVALUATOR_SCHEMA_PATH = "key.evaluator.schema.path";
    public static final String KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLED =
            "key.evaluator.schema.registry.enabled";

    public static final String VALUE_EVALUATOR_TYPE = "value.evaluator.type";

    public static final String VALUE_EVALUATOR_SCHEMA_PATH = "value.evaluator.schema.path";
    public static final String VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLED =
            "key.evaluator.schema.registry.enabled";

    public static final String KEY_EVALUATOR_SCHEMA_REGISTRY_URL =
            "key.evaluator.schema.registry.url";

    public static final String VALUE_EVALUATOR_SCHEMA_REGISTRY_URL =
            "value.evaluator.schema.registry.url";

    public static final String ITEM_INFO_NAME = "info.item";

    public static final String ITEM_INFO_FIELD = "info.field";

    public static final String RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY =
            "record.extraction.error.strategy";

    public static final String ENABLE_ENCRYTPTION = "encryption.enabled";

    public static final String ENABLE_AUTHENTICATION = "authentication.enabled";

    // Kafka consumer specific settings
    private static final String CONNECTOR_PREFIX = "consumer.";
    public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG =
            CONNECTOR_PREFIX + AUTO_OFFSET_RESET_CONFIG;
    public static final String CONSUMER_ENABLE_AUTO_COMMIT_CONFIG =
            CONNECTOR_PREFIX + ENABLE_AUTO_COMMIT_CONFIG;
    public static final String CONSUMER_FETCH_MIN_BYTES_CONFIG =
            CONNECTOR_PREFIX + FETCH_MIN_BYTES_CONFIG;
    public static final String CONSUMER_FETCH_MAX_BYTES_CONFIG =
            CONNECTOR_PREFIX + FETCH_MAX_BYTES_CONFIG;
    public static final String CONSUMER_FETCH_MAX_WAIT_MS_CONFIG =
            CONNECTOR_PREFIX + FETCH_MAX_WAIT_MS_CONFIG;
    public static final String CONSUMER_MAX_POLL_RECORDS =
            CONNECTOR_PREFIX + MAX_POLL_RECORDS_CONFIG;
    public static final String CONSUMER_RECONNECT_BACKOFF_MS_CONFIG =
            CONNECTOR_PREFIX + RECONNECT_BACKOFF_MS_CONFIG;
    public static final String CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG =
            CONNECTOR_PREFIX + RECONNECT_BACKOFF_MAX_MS_CONFIG;
    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS =
            CONNECTOR_PREFIX + HEARTBEAT_INTERVAL_MS_CONFIG;
    public static final String CONSUMER_SESSION_TIMEOUT_MS =
            CONNECTOR_PREFIX + SESSION_TIMEOUT_MS_CONFIG;
    public static final String CONSUMER_MAX_POLL_INTERVAL_MS =
            CONNECTOR_PREFIX + MAX_POLL_INTERVAL_MS_CONFIG;
    public static final String CONSUMER_METADATA_MAX_AGE_CONFIG =
            CONNECTOR_PREFIX + METADATA_MAX_AGE_CONFIG;
    public static final String CONSUMER_REQUEST_TIMEOUT_MS_CONFIG =
            CONNECTOR_PREFIX + REQUEST_TIMEOUT_MS_CONFIG;
    public static final String CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG =
            CONNECTOR_PREFIX + DEFAULT_API_TIMEOUT_MS_CONFIG;
    public static final String CONSUMER_RETRIES =
            CONNECTOR_PREFIX + AdminClientConfig.RETRIES_CONFIG;

    private static final ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("root")
                        .add(ADAPTERS_CONF_ID, true, false, TEXT)
                        .add(ENABLED, false, false, BOOL, defaultValue("true"))
                        .add(ADAPTER_DIR, true, false, ConfType.DIRECTORY)
                        .add(BOOTSTRAP_SERVERS, true, false, ConfType.HOST_LIST)
                        .add(
                                GROUP_ID,
                                false,
                                false,
                                TEXT,
                                defaultValue(
                                        params -> {
                                            String suffix =
                                                    new SecureRandom()
                                                            .ints(20, 48, 122)
                                                            .mapToObj(Character::toString)
                                                            .collect(Collectors.joining());
                                            return "%s-%s-%s"
                                                    .formatted(
                                                            params.get(ADAPTERS_CONF_ID),
                                                            params.get(DATA_ADAPTER_NAME),
                                                            suffix);
                                        }))
                        .add(DATA_ADAPTER_NAME, true, false, TEXT)
                        .add(ITEM_TEMPLATE, true, true, TEXT)
                        .add(TOPIC_MAPPING, true, true, MAP_SUFFIX, TEXT)
                        .add(FIELD_MAPPING, true, true, TEXT)
                        .add(KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, URL)
                        .add(VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false, false, URL)
                        .add(
                                KEY_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(EvaluatorType.STRING.toString()))
                        .add(KEY_EVALUATOR_SCHEMA_PATH, false, false, FILE)
                        .add(
                                KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLED,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(
                                VALUE_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(EvaluatorType.STRING.toString()))
                        .add(VALUE_EVALUATOR_SCHEMA_PATH, false, false, FILE)
                        .add(
                                VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLED,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(ITEM_INFO_NAME, false, false, TEXT, defaultValue("INFO"))
                        .add(ITEM_INFO_FIELD, false, false, TEXT, defaultValue("MSG"))
                        .add(
                                RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY,
                                false,
                                false,
                                ERROR_STRATEGY,
                                defaultValue("IGNORE_AND_CONTINUE"))
                        .add(ENABLE_ENCRYTPTION, false, false, BOOL, defaultValue("false"))
                        .add(ENABLE_AUTHENTICATION, false, false, BOOL, defaultValue("false"))
                        .add(
                                CONSUMER_AUTO_OFFSET_RESET_CONFIG,
                                false,
                                false,
                                TEXT,
                                defaultValue("latest"))
                        .add(
                                CONSUMER_ENABLE_AUTO_COMMIT_CONFIG,
                                false,
                                false,
                                BOOL,
                                false,
                                defaultValue("false"))
                        .add(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG, false, false, INT)
                        .add(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MIN_BYTES_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MIN_BYTES_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MAX_BYTES_CONFIG, false, false, INT)
                        .add(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG, false, false, INT)
                        .add(CONSUMER_MAX_POLL_RECORDS, false, false, INT)
                        .add(CONSUMER_HEARTBEAT_INTERVAL_MS, false, false, INT)
                        .add(CONSUMER_SESSION_TIMEOUT_MS, false, false, INT)
                        .add(
                                CONSUMER_MAX_POLL_INTERVAL_MS,
                                false,
                                false,
                                INT,
                                false,
                                defaultValue("5000"))
                        .add(
                                CONSUMER_METADATA_MAX_AGE_CONFIG,
                                false,
                                false,
                                INT,
                                false,
                                defaultValue("250"))
                        .add(
                                CONSUMER_REQUEST_TIMEOUT_MS_CONFIG,
                                false,
                                false,
                                INT,
                                false,
                                defaultValue("1000"))
                        .add(CONSUMER_RETRIES, false, false, INT, false, defaultValue("0"))
                        .add(
                                CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG,
                                false,
                                false,
                                INT,
                                false,
                                defaultValue("15000"))
                        .withEnabledChildConfigs(EncryptionConfigs.spec(), ENABLE_ENCRYTPTION)
                        .withEnabledChildConfigs(
                                BrokerAuthenticationConfigs.spec(), ENABLE_AUTHENTICATION);
    }

    private ConnectorConfig(ConfigsSpec spec, Map<String, String> configs) {
        super(spec, configs);
    }

    public ConnectorConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);
    }

    @Override
    protected void validate() throws ConfigException {
        if (isEncryptionEnabled()) {
            // If a truststore file is provided, a password must be provided as well
            getTrustStorePassword();
        }

        if (isAuthenticationEnabled()) {
            if (isGssapiEnabled()) {
                // If use keytab, a key tab file must be provided
                gssapiKeyTab();
            } else {
                // In case of PLAIN auhentication, credentials must be provided
                getAuthenticationUsername();
                getAuthenticationPassword();
            }
        }
    }

    static ConfigsSpec configSpec() {
        return CONFIG_SPEC;
    }

    public static ConnectorConfig newConfig(File adapterDir, Map<String, String> params) {
        return new ConnectorConfig(
                AbstractConfig.appendAdapterDir(CONFIG_SPEC, params, adapterDir));
    }

    public Properties baseConsumerProps() {
        NoNullKeyProperties properties = new NoNullKeyProperties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, getHostsList(BOOTSTRAP_SERVERS));
        properties.setProperty(GROUP_ID_CONFIG, getText(GROUP_ID));
        properties.setProperty(METADATA_MAX_AGE_CONFIG, getInt(CONSUMER_METADATA_MAX_AGE_CONFIG));
        properties.setProperty(
                AUTO_OFFSET_RESET_CONFIG, getText(CONSUMER_AUTO_OFFSET_RESET_CONFIG));
        properties.setProperty(
                ENABLE_AUTO_COMMIT_CONFIG, getBooleanStr(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG));
        properties.setProperty(FETCH_MIN_BYTES_CONFIG, getInt(CONSUMER_FETCH_MIN_BYTES_CONFIG));
        properties.setProperty(FETCH_MAX_BYTES_CONFIG, getInt(CONSUMER_FETCH_MAX_BYTES_CONFIG));
        properties.setProperty(FETCH_MAX_WAIT_MS_CONFIG, getInt(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG));
        properties.setProperty(
                HEARTBEAT_INTERVAL_MS_CONFIG, getInt(CONSUMER_HEARTBEAT_INTERVAL_MS));
        properties.setProperty(MAX_POLL_RECORDS_CONFIG, getInt(CONSUMER_MAX_POLL_RECORDS));
        properties.setProperty(
                RECONNECT_BACKOFF_MAX_MS_CONFIG, getInt(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG));
        properties.setProperty(
                RECONNECT_BACKOFF_MS_CONFIG, getInt(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG));
        properties.setProperty(SESSION_TIMEOUT_MS_CONFIG, getInt(CONSUMER_SESSION_TIMEOUT_MS));
        properties.setProperty(MAX_POLL_INTERVAL_MS_CONFIG, getInt(CONSUMER_MAX_POLL_INTERVAL_MS));
        properties.setProperty(
                REQUEST_TIMEOUT_MS_CONFIG, getInt(CONSUMER_REQUEST_TIMEOUT_MS_CONFIG));
        properties.setProperty(
                DEFAULT_API_TIMEOUT_MS_CONFIG, getInt(CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG));
        properties.setProperty(AdminClientConfig.RETRIES_CONFIG, getInt(CONSUMER_RETRIES));

        properties.putAll(EncryptionConfigs.addEncryption(this));
        properties.putAll(BrokerAuthenticationConfigs.addAuthentication(this));
        return properties.properties();
    }

    public Map<String, ?> extendsConsumerProps(Map<String, String> props) {
        Map<String, String> extendedProps =
                new HashMap<>(
                        baseConsumerProps().entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                e -> e.getKey().toString(),
                                                e -> e.getValue().toString())));
        extendedProps.putAll(props);
        return extendedProps;
    }

    public final EvaluatorType getEvaluator(String configKey) {
        return EvaluatorType.valueOf(get(configKey, EVALUATOR, false));
    }

    public final RecordErrorHandlingStrategy getRecordExtractionErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.valueOf(
                get(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, ERROR_STRATEGY, false));
    }

    public boolean hasKeySchemaFile() {
        return getFile(KEY_EVALUATOR_SCHEMA_PATH) != null;
    }

    public boolean isSchemaRegistryEnabledForKey() {
        return getBoolean(KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLED);
    }

    public boolean hasValueSchemaFile() {
        return getFile(VALUE_EVALUATOR_SCHEMA_PATH) != null;
    }

    public boolean isSchemaRegistryEnabledForValue() {
        return getBoolean(VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLED);
    }

    public boolean isSchemaRegistryEnabled() {
        return isSchemaRegistryEnabledForKey() || isSchemaRegistryEnabledForValue();
    }

    public String getMetadataAdapterName() {
        return getText(ADAPTERS_CONF_ID);
    }

    public String getAdapterName() {
        return getText(DATA_ADAPTER_NAME);
    }

    public boolean isEnabled() {
        return getBoolean(ENABLED);
    }

    public String getItemInfoName() {
        return getText(ITEM_INFO_NAME);
    }

    public String getItemInfoField() {
        return getText(ITEM_INFO_FIELD);
    }

    public boolean isEncryptionEnabled() {
        return getBoolean(ENABLE_ENCRYTPTION);
    }

    public boolean isKeystoreEnabled() {
        checkEncryptionEnabled();
        return getBoolean(EncryptionConfigs.ENABLE_MTLS);
    }

    private void checkEncryptionEnabled() {
        if (!isEncryptionEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not enabled".formatted(ENABLE_ENCRYTPTION));
        }
    }

    private void checkKeystoreEnabled() {
        if (!isKeystoreEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not enabled".formatted(EncryptionConfigs.ENABLE_MTLS));
        }
    }

    private void checkAuthenticationEnabled() {
        if (!isAuthenticationEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not enabled".formatted(ENABLE_AUTHENTICATION));
        }
    }

    public List<SslProtocol> getEnabledProtocols() {
        return SslProtocol.fromValueStr(getEnabledProtocolsAsStr());
    }

    public String getEnabledProtocolsAsStr() {
        checkEncryptionEnabled();
        return get(EncryptionConfigs.SSL_ENABLED_PROTOCOLS, ConfType.SSL_ENABLED_PROTOCOLS, false);
    }

    public SslProtocol getSslProtocol() {
        checkEncryptionEnabled();
        return SslProtocol.fromName(
                get(EncryptionConfigs.SSL_PROTOCOL, ConfType.SSL_PROTOCOL, false));
    }

    public KeystoreType getTrustStoreType() {
        checkEncryptionEnabled();
        return KeystoreType.valueOf(
                get(EncryptionConfigs.TRUSTSTORE_TYPE, ConfType.KEYSTORE_TYPE, false));
    }

    public String getTrustStorePath() {
        checkEncryptionEnabled();
        return getFile(EncryptionConfigs.TRUSTSTORE_PATH);
    }

    public String getTrustStorePassword() {
        checkEncryptionEnabled();
        boolean isRequired = getTrustStorePath() != null;
        return getText(EncryptionConfigs.TRUSTSTORE_PASSWORD, isRequired);
    }

    public List<String> getCipherSuites() {
        checkEncryptionEnabled();
        return getTextList(EncryptionConfigs.SSL_CIPHER_SUITES);
    }

    public String getCipherSuitesAsStr() {
        checkEncryptionEnabled();
        return get(EncryptionConfigs.SSL_CIPHER_SUITES, ConfType.TEXT_LIST, false);
    }

    public String getSslProvider() {
        checkEncryptionEnabled();
        return getText(EncryptionConfigs.SSL_PROVIDER);
    }

    public boolean isHostNameVerificationEnabled() {
        checkEncryptionEnabled();
        return getBoolean(EncryptionConfigs.ENABLE_HOSTNAME_VERIFICATION);
    }

    public KeystoreType keystoreType() {
        checkKeystoreEnabled();
        return KeystoreType.valueOf(
                get(EncryptionConfigs.KEYSTORE_TYPE, ConfType.KEYSTORE_TYPE, false));
    }

    public String keystorePath() {
        checkKeystoreEnabled();
        return getFile(EncryptionConfigs.KEYSTORE_PATH);
    }

    public String keystorePassword() {
        checkKeystoreEnabled();
        return getText(EncryptionConfigs.KEYSTORE_PASSWORD);
    }

    public String keyPassword() {
        checkKeystoreEnabled();
        return getText(EncryptionConfigs.KEY_PASSWORD);
    }

    public boolean isAuthenticationEnabled() {
        return getBoolean(ENABLE_AUTHENTICATION);
    }

    public SaslMechanism getAuthenticationMechanism() {
        checkAuthenticationEnabled();
        return SaslMechanism.valueOf(
                get(BrokerAuthenticationConfigs.SASL_MECHANISM, ConfType.SASL_MECHANISM, false));
    }

    public String getAuthenticationUsername() {
        checkAuthenticationEnabled();
        boolean isRequired = !isGssapiEnabled();
        return getText(BrokerAuthenticationConfigs.USERNAME, isRequired);
    }

    public String getAuthenticationPassword() {
        checkAuthenticationEnabled();
        boolean isRequired = !isGssapiEnabled();
        return getText(BrokerAuthenticationConfigs.PASSWORD, isRequired);
    }

    public boolean isGssapiEnabled() {
        return getAuthenticationMechanism().equals(SaslMechanism.GSSAPI);
    }

    private void checkGssapi() {
        if (!isGssapiEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not set to GSSAPI"
                            .formatted(BrokerAuthenticationConfigs.SASL_MECHANISM));
        }
    }

    public boolean gssapiUseKeyTab() {
        checkGssapi();
        return getBoolean(BrokerAuthenticationConfigs.GSSAPI_USE_KEY_TAB);
    }

    public String gssapiKeyTab() {
        checkGssapi();
        boolean isRequired = gssapiUseKeyTab();
        return getFile(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB, isRequired);
    }

    public boolean gssapiStoreKey() {
        checkGssapi();
        return getBoolean(BrokerAuthenticationConfigs.GSSAPI_STORE_KEY);
    }

    public String gssapiPrincipal() {
        checkGssapi();
        return getText(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL);
    }

    public String gssapiKerberosServiceName() {
        checkGssapi();
        return getText(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME);
    }

    public void checkSchemaRegistryEnabled() {
        if (!isSchemaRegistryEnabled()) {
            throw new ConfigException(
                    "Parameters [%s] and [%s] are both not enabled"
                            .formatted(
                                    KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLED,
                                    VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLED));
        }
    }

    public String schemaRegistryUrl() {
        return getHost(SchemaRegistryConfigs.URL);
    }

    public boolean isSchemaRegistryEncryptionEnabled() {
        checkSchemaRegistryEnabled();
        return getBoolean(SchemaRegistryConfigs.ENABLE_ENCRYTPTION);
    }

    public void checkSchemaRegistryEncryptionEnabled() {
        if (!isSchemaRegistryEncryptionEnabled()) {
            throw new ConfigException(
                    "Parameters [%s] is not enabled"
                            .formatted(SchemaRegistryConfigs.ENABLE_ENCRYTPTION));
        }
    }

    public List<SslProtocol> schemaRegistryEnabledProtocols() {
        return SslProtocol.fromValueStr(schemaRegistryEnabledProtocolsAsStr());
    }

    public String schemaRegistryEnabledProtocolsAsStr() {
        checkEncryptionEnabled();
        return get(
                SchemaRegistryConfigs.SSL_ENABLED_PROTOCOLS, ConfType.SSL_ENABLED_PROTOCOLS, false);
    }

    public SslProtocol schemaRegistrySslProtocol() {
        checkSchemaRegistryEncryptionEnabled();
        return SslProtocol.fromName(
                get(SchemaRegistryConfigs.SSL_PROTOCOL, ConfType.SSL_PROTOCOL, false));
    }

    public KeystoreType schemaRegistryTruststoreType() {
        checkSchemaRegistryEncryptionEnabled();
        return KeystoreType.valueOf(
                get(SchemaRegistryConfigs.TRUSTSTORE_TYPE, ConfType.KEYSTORE_TYPE, false));
    }

    public String schemaRegistryTrustStorePath() {
        checkSchemaRegistryEncryptionEnabled();
        return getFile(SchemaRegistryConfigs.TRUSTSTORE_PATH);
    }

    public String schemaRegistryTrustStorePassword() {
        checkSchemaRegistryKeystoreEnabled();
        boolean isRequired = schemaRegistryTrustStorePath() != null;
        return getText(SchemaRegistryConfigs.KEYSTORE_PASSWORD, isRequired);
    }

    public List<String> schemaRegistryCipherSuites() {
        checkSchemaRegistryEncryptionEnabled();
        return getTextList(SchemaRegistryConfigs.SSL_CIPHER_SUITES);
    }

    public String schemaRegistryCipherSuitesAsStr() {
        checkSchemaRegistryEncryptionEnabled();
        return get(SchemaRegistryConfigs.SSL_CIPHER_SUITES, ConfType.TEXT_LIST, false);
    }

    public String schemaRegistrySslProvider() {
        checkSchemaRegistryEncryptionEnabled();
        return getText(SchemaRegistryConfigs.SSL_PROVIDER);
    }

    public boolean isSchemaRegistryHostNameVerificationEnabled() {
        checkSchemaRegistryEncryptionEnabled();
        return getBoolean(SchemaRegistryConfigs.ENABLE_HOSTNAME_VERIFICATION);
    }

    public String schemaRegistryKeyPassword() {
        checkSchemaRegistryKeystoreEnabled();
        return getText(SchemaRegistryConfigs.KEY_PASSWORD);
    }

    public boolean isSchemaRegistryKeystoreEnabled() {
        checkSchemaRegistryEncryptionEnabled();
        return getBoolean(SchemaRegistryConfigs.ENABLE_MTLS);
    }

    public void checkSchemaRegistryKeystoreEnabled() {
        if (!isSchemaRegistryKeystoreEnabled()) {
            throw new ConfigException(
                    "Parameters [%s] is not enabled".formatted(SchemaRegistryConfigs.ENABLE_MTLS));
        }
    }

    public KeystoreType schemaRegistryKeystoreType() {
        checkSchemaRegistryKeystoreEnabled();
        return KeystoreType.valueOf(
                get(SchemaRegistryConfigs.KEYSTORE_TYPE, ConfType.KEYSTORE_TYPE, false));
    }

    public String schemaRegistryKeystorePath() {
        checkSchemaRegistryKeystoreEnabled();
        return getFile(SchemaRegistryConfigs.KEYSTORE_PATH);
    }

    public String schemaRegistryKeystorePassword() {
        checkSchemaRegistryKeystoreEnabled();
        return getText(SchemaRegistryConfigs.KEYSTORE_PASSWORD);
    }
}
