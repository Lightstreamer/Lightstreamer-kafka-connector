
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CONSUME_FROM;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.ERROR_STRATEGY;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
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

import com.lightstreamer.kafka.adapters.commons.NonNullKeyProperties;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordComsumeFrom;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.EnablingKey;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.config.FieldConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.ItemTemplateConfigs;
import com.lightstreamer.kafka.common.config.TopicConfigurations.TopicMappingConfig;
import com.lightstreamer.kafka.common.utils.Split;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.io.File;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public final class ConnectorConfig extends AbstractConfig {

    static final String LIGHSTREAMER_CLIENT_ID = "cwc|5795fea5-2ddf-41c7-b44c-c6cb0982d7b|";

    public static final String ENABLE = "enable";

    public static final String DATA_ADAPTER_NAME = "data_provider.name";

    public static final String GROUP_ID = "group.id";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String ITEM_TEMPLATE = "item-template";

    public static final String TOPIC_MAPPING = "map";
    private static final String MAP_SUFFIX = "to";

    public static final String FIELD_MAPPING = "field";

    public static final String RECORD_KEY_EVALUATOR_TYPE = "record.key.evaluator.type";
    public static final String RECORD_KEY_EVALUATOR_SCHEMA_PATH =
            "record.key.evaluator.schema.path";
    public static final String RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE =
            "record.key.evaluator.schema.registry.enable";

    public static final String RECORD_VALUE_EVALUATOR_TYPE = "record.value.evaluator.type";
    public static final String RECORD_VALUE_EVALUATOR_SCHEMA_PATH =
            "record.value.evaluator.schema.path";
    public static final String RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE =
            "record.value.evaluator.schema.registry.enable";

    public static final String RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY =
            "record.extraction.error.strategy";

    public static final String ITEM_INFO_NAME = "info.item";

    public static final String ITEM_INFO_FIELD = "info.field";

    public static final String ENCYRPTION_ENABLE = "encryption.enable";

    public static final String AUTHENTICATION_ENABLE = "authentication.enable";

    // Kafka consumer specific settings
    private static final String CONNECTOR_PREFIX = "consumer.";
    public static final String RECORD_CONSUME_FROM = "record.consume.from";
    public static final String CONSUMER_CLIENT_ID =
            CONNECTOR_PREFIX + CommonClientConfigs.CLIENT_ID_CONFIG;
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
                        .add(ADAPTER_DIR, true, false, ConfType.DIRECTORY)
                        .add(DATA_ADAPTER_NAME, true, false, TEXT)
                        .add(ENABLE, false, false, BOOL, defaultValue("true"))
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
                                                            .filter(Character::isLetterOrDigit)
                                                            .mapToObj(Character::toString)
                                                            .collect(Collectors.joining());
                                            return "%s-%s-%s"
                                                    .formatted(
                                                            params.get(ADAPTERS_CONF_ID),
                                                            params.get(DATA_ADAPTER_NAME),
                                                            suffix);
                                        }))
                        .add(ITEM_TEMPLATE, false, true, TEXT)
                        .add(TOPIC_MAPPING, true, true, MAP_SUFFIX, TEXT_LIST)
                        .add(FIELD_MAPPING, true, true, TEXT)
                        .add(
                                RECORD_KEY_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(EvaluatorType.STRING.toString()))
                        .add(RECORD_KEY_EVALUATOR_SCHEMA_PATH, false, false, FILE)
                        .add(
                                RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(
                                RECORD_VALUE_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(EvaluatorType.STRING.toString()))
                        .add(RECORD_VALUE_EVALUATOR_SCHEMA_PATH, false, false, FILE)
                        .add(
                                RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
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
                        .add(ENCYRPTION_ENABLE, false, false, BOOL, defaultValue("false"))
                        .add(AUTHENTICATION_ENABLE, false, false, BOOL, defaultValue("false"))
                        .add(
                                RECORD_CONSUME_FROM,
                                false,
                                false,
                                CONSUME_FROM,
                                defaultValue(RecordComsumeFrom.LATEST.toString()))
                        .add(
                                CONSUMER_CLIENT_ID,
                                false,
                                false,
                                TEXT,
                                false,
                                defaultValue(
                                        params -> {
                                            String hostList = params.get(BOOTSTRAP_SERVERS);
                                            if (hostList == null) {
                                                return "";
                                            }
                                            if (Split.byComma(hostList).stream()
                                                    .flatMap(s -> Split.asPair(s, ':').stream())
                                                    .map(p -> p.key())
                                                    .allMatch(
                                                            s -> s.endsWith(".confluent.cloud"))) {
                                                return LIGHSTREAMER_CLIENT_ID;
                                            }
                                            return "";
                                        }))
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
                                defaultValue("30000"))
                        .add(CONSUMER_RETRIES, false, false, INT, false, defaultValue("0"))
                        .add(
                                CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG,
                                false,
                                false,
                                INT,
                                false,
                                defaultValue("60000"))
                        .withEnabledChildConfigs(EncryptionConfigs.spec(), ENCYRPTION_ENABLE)
                        .withEnabledChildConfigs(
                                BrokerAuthenticationConfigs.spec(), AUTHENTICATION_ENABLE)
                        .withEnabledChildConfigs(
                                SchemaRegistryConfigs.spec(),
                                EnablingKey.of(
                                        RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE));
    }

    private final Properties consumerProps;
    private final ItemTemplateConfigs itemTemplateConfigs;
    private final List<TopicMappingConfig> topicMappings;

    private FieldConfigs fieldConfigs;

    private ConnectorConfig(ConfigsSpec spec, Map<String, String> configs) {
        super(spec, configs);
        this.consumerProps = initProps();
        itemTemplateConfigs = ItemTemplateConfigs.from(getValues(ITEM_TEMPLATE));
        topicMappings = TopicMappingConfig.from(getValues(TOPIC_MAPPING));
        fieldConfigs = FieldConfigs.from(getValues(FIELD_MAPPING));
    }

    public ConnectorConfig(Map<String, String> configs) {
        this(CONFIG_SPEC, configs);
    }

    @Override
    protected final void postValidate() throws ConfigException {
        checkAvroSchemaConfig(true);
        checkAvroSchemaConfig(false);
    }

    private void checkAvroSchemaConfig(boolean isKey) {
        String schemaPathKey =
                isKey ? RECORD_KEY_EVALUATOR_SCHEMA_PATH : RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
        String evaluatorKey = isKey ? RECORD_KEY_EVALUATOR_TYPE : RECORD_VALUE_EVALUATOR_TYPE;
        String schemaEnabledKey =
                isKey
                        ? RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE
                        : RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
        if (getEvaluator(evaluatorKey).equals(EvaluatorType.AVRO)) {
            if (!getBoolean(schemaEnabledKey)) {
                try {
                    getFile(schemaPathKey, true);
                } catch (ConfigException ce) {
                    throw new ConfigException(
                            "Specify a valid value either for [%s] or [%s]"
                                    .formatted(schemaPathKey, schemaEnabledKey));
                }
            }
        }
    }

    private Properties initProps() {
        NonNullKeyProperties properties = new NonNullKeyProperties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, getHostsList(BOOTSTRAP_SERVERS));
        properties.setProperty(GROUP_ID_CONFIG, getText(GROUP_ID));
        properties.setProperty(CLIENT_ID_CONFIG, getText(CONSUMER_CLIENT_ID));
        properties.setProperty(METADATA_MAX_AGE_CONFIG, getInt(CONSUMER_METADATA_MAX_AGE_CONFIG));
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, getRecordConsumeFrom().toPropertyValue());
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
        properties.putAll(SchemaRegistryConfigs.addSchemaRegistry(this));

        return properties.unmodifiables();
    }

    static ConfigsSpec configSpec() {
        return CONFIG_SPEC;
    }

    public static ConnectorConfig newConfig(File adapterDir, Map<String, String> params) {
        return new ConnectorConfig(
                AbstractConfig.appendAdapterDir(CONFIG_SPEC, params, adapterDir));
    }

    public Properties baseConsumerProps() {
        return consumerProps;
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

    public final EvaluatorType getKeyEvaluator() {
        return EvaluatorType.valueOf(get(RECORD_KEY_EVALUATOR_TYPE, EVALUATOR, false));
    }

    public final EvaluatorType getValueEvaluator() {
        return EvaluatorType.valueOf(get(RECORD_VALUE_EVALUATOR_TYPE, EVALUATOR, false));
    }

    public final EvaluatorType getEvaluator(String configKey) {
        return EvaluatorType.valueOf(get(configKey, EVALUATOR, false));
    }

    public final RecordComsumeFrom getRecordConsumeFrom() {
        return RecordComsumeFrom.valueOf(get(RECORD_CONSUME_FROM, CONSUME_FROM, false));
    }

    public final RecordErrorHandlingStrategy getRecordExtractionErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.valueOf(
                get(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, ERROR_STRATEGY, false));
    }

    public boolean hasKeySchemaFile() {
        return getFile(RECORD_KEY_EVALUATOR_SCHEMA_PATH) != null;
    }

    public boolean isSchemaRegistryEnabledForKey() {
        return getBoolean(RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
    }

    public boolean hasValueSchemaFile() {
        return getFile(RECORD_VALUE_EVALUATOR_SCHEMA_PATH) != null;
    }

    public boolean isSchemaRegistryEnabledForValue() {
        return getBoolean(RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE);
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
        return getBoolean(ENABLE);
    }

    public String getItemInfoName() {
        return getText(ITEM_INFO_NAME);
    }

    public String getItemInfoField() {
        return getText(ITEM_INFO_FIELD);
    }

    public boolean isEncryptionEnabled() {
        return getBoolean(ENCYRPTION_ENABLE);
    }

    public boolean isKeystoreEnabled() {
        checkEncryptionEnabled();
        return getBoolean(EncryptionConfigs.ENABLE_MTLS);
    }

    private void checkEncryptionEnabled() {
        if (!isEncryptionEnabled()) {
            throw new ConfigException(
                    "Encryption is not enabled. Check parameter [%s]".formatted(ENCYRPTION_ENABLE));
        }
    }

    private void checkKeystoreEnabled() {
        if (!isKeystoreEnabled()) {
            throw new ConfigException(
                    "Key store is not enabled. Check parameter [%s]"
                            .formatted(EncryptionConfigs.ENABLE_MTLS));
        }
    }

    private void checkAuthenticationEnabled() {
        if (!isAuthenticationEnabled()) {
            throw new ConfigException(
                    "Authentication is not enabled. Check parameter [%s]"
                            .formatted(AUTHENTICATION_ENABLE));
        }
    }

    public List<SslProtocol> enabledProtocols() {
        return SslProtocol.fromValueStr(enabledProtocolsAsStr());
    }

    public String enabledProtocolsAsStr() {
        checkEncryptionEnabled();
        return get(EncryptionConfigs.SSL_ENABLED_PROTOCOLS, ConfType.SSL_ENABLED_PROTOCOLS, false);
    }

    public SslProtocol sslProtocol() {
        checkEncryptionEnabled();
        return SslProtocol.fromName(
                get(EncryptionConfigs.SSL_PROTOCOL, ConfType.SSL_PROTOCOL, false));
    }

    public KeystoreType truststoreType() {
        checkEncryptionEnabled();
        return KeystoreType.valueOf(
                get(EncryptionConfigs.TRUSTSTORE_TYPE, ConfType.KEYSTORE_TYPE, false));
    }

    public String truststorePath() {
        checkEncryptionEnabled();
        return getFile(EncryptionConfigs.TRUSTSTORE_PATH);
    }

    public String truststorePassword() {
        checkEncryptionEnabled();
        return getText(EncryptionConfigs.TRUSTSTORE_PASSWORD);
    }

    public List<String> cipherSuites() {
        checkEncryptionEnabled();
        return getTextList(EncryptionConfigs.SSL_CIPHER_SUITES);
    }

    public String cipherSuitesAsStr() {
        checkEncryptionEnabled();
        return get(EncryptionConfigs.SSL_CIPHER_SUITES, ConfType.TEXT_LIST, false);
    }

    public String sslProvider() {
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
        return getBoolean(AUTHENTICATION_ENABLE);
    }

    public SaslMechanism authenticationMechanism() {
        checkAuthenticationEnabled();
        return SaslMechanism.fromName(
                get(BrokerAuthenticationConfigs.SASL_MECHANISM, ConfType.SASL_MECHANISM, false));
    }

    public String authenticationUsername() {
        checkAuthenticationEnabled();
        boolean isRequired = !isGssapiEnabled();
        return getText(BrokerAuthenticationConfigs.USERNAME, isRequired);
    }

    public String authenticationPassword() {
        checkAuthenticationEnabled();
        boolean isRequired = !isGssapiEnabled();
        return getText(BrokerAuthenticationConfigs.PASSWORD, isRequired);
    }

    public boolean isGssapiEnabled() {
        return authenticationMechanism().equals(SaslMechanism.GSSAPI);
    }

    private void checkGssapi() {
        if (!isGssapiEnabled()) {
            throw new ConfigException(
                    "GSSAPI is not configured. Check parameter [%s]"
                            .formatted(BrokerAuthenticationConfigs.SASL_MECHANISM));
        }
    }

    public boolean gssapiUseKeyTab() {
        checkGssapi();
        return getBoolean(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE);
    }

    public String gssapiKeyTab() {
        boolean isRequired = gssapiUseKeyTab();
        return getFile(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH, isRequired);
    }

    public boolean gssapiStoreKey() {
        checkGssapi();
        return getBoolean(BrokerAuthenticationConfigs.GSSAPI_STORE_KEY_ENABLE);
    }

    public String gssapiPrincipal() {
        boolean isRequired = !gssapiUseTicketCache();
        return getText(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL, isRequired);
    }

    public String gssapiKerberosServiceName() {
        checkGssapi();
        return getText(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME);
    }

    public boolean gssapiUseTicketCache() {
        checkGssapi();
        return getBoolean(BrokerAuthenticationConfigs.GSSAPI_TICKET_CACHE_ENABLE);
    }

    private void checkSchemaRegistryEnabled() {
        if (!isSchemaRegistryEnabled()) {
            throw new ConfigException(
                    "Neither parameter [%s] nor parameter [%s] are enabled"
                            .formatted(
                                    RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                                    RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE));
        }
    }

    public String schemaRegistryUrl() {
        checkSchemaRegistryEnabled();
        return getUrl(SchemaRegistryConfigs.URL);
    }

    public boolean isSchemaRegistryEncryptionEnabled() {
        checkSchemaRegistryEnabled();
        return schemaRegistryUrl().startsWith("https");
    }

    private void checkSchemaRegistryEncryptionEnabled() {
        if (!isSchemaRegistryEncryptionEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not set with https protocol"
                            .formatted(SchemaRegistryConfigs.URL));
        }
    }

    public List<SslProtocol> schemaRegistryEnabledProtocols() {
        return SslProtocol.fromValueStr(schemaRegistryEnabledProtocolsAsStr());
    }

    public String schemaRegistryEnabledProtocolsAsStr() {
        checkSchemaRegistryEncryptionEnabled();
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

    public String schemaRegistryTruststorePath() {
        checkSchemaRegistryEncryptionEnabled();
        return getFile(SchemaRegistryConfigs.TRUSTSTORE_PATH);
    }

    public String schemaRegistryTruststorePassword() {
        checkSchemaRegistryEnabled();
        boolean isRequired = schemaRegistryTruststorePath() != null;
        return getText(SchemaRegistryConfigs.TRUSTSTORE_PASSWORD, isRequired);
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
        return getBoolean(SchemaRegistryConfigs.HOSTNAME_VERIFICATION_ENANLE);
    }

    public String schemaRegistryKeyPassword() {
        checkSchemaRegistryKeystoreEnabled();
        return getText(SchemaRegistryConfigs.KEY_PASSWORD);
    }

    public boolean isSchemaRegistryKeystoreEnabled() {
        checkSchemaRegistryEncryptionEnabled();
        return getBoolean(SchemaRegistryConfigs.KEYSTORE_ENABLE);
    }

    private void checkSchemaRegistryKeystoreEnabled() {
        if (!isSchemaRegistryKeystoreEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not enabled"
                            .formatted(SchemaRegistryConfigs.KEYSTORE_ENABLE));
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

    public boolean isSchemaRegistryBasicAuthenticationEnabled() {
        checkSchemaRegistryEnabled();
        return getBoolean(SchemaRegistryConfigs.ENABLE_BASIC_AUTHENTICATION);
    }

    private void checkSchemaRegistryBasicAuthentication() {
        if (!isSchemaRegistryBasicAuthenticationEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not enabled"
                            .formatted(SchemaRegistryConfigs.ENABLE_BASIC_AUTHENTICATION));
        }
    }

    public String schemaRegistryBasicAuthenticationUserName() {
        checkSchemaRegistryBasicAuthentication();
        return getText(SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_NAME);
    }

    public String schemaRegistryBasicAuthenticationPassword() {
        checkSchemaRegistryBasicAuthentication();
        return getText(SchemaRegistryConfigs.BASIC_AUTHENTICATION_USER_PASSWORD);
    }

    public ItemTemplateConfigs getItemTemplateConfigs() {
        return itemTemplateConfigs;
    }

    public List<TopicMappingConfig> getTopicMappings() {
        return topicMappings;
    }

    public FieldConfigs getFieldConfigs() {
        return fieldConfigs;
    }
}
