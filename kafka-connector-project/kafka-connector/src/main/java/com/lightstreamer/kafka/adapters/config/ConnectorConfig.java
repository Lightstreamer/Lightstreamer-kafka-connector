
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BLANKABLE_TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CHAR;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.CONSUME_FROM;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.ERROR_STRATEGY;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.EVALUATOR;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.GROUP_MODE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.NON_NEGATIVE_INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.ORDER_STRATEGY;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.POSITIVE_INT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT_LIST;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.THREADS;
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
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.ConsumerGroupMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SchemaRegistryProvider;
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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public final class ConnectorConfig extends AbstractConfig {

    static final String LIGHTSTREAMER_CLIENT_ID = "cwc|5795fea5-2ddf-41c7-b44c-c6cb0982d7b|";

    public static final String ENABLE = "enable";

    public static final String DATA_ADAPTER_NAME = "data_provider.name";

    public static final String GROUP_ID = "group.id";

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String ITEM_TEMPLATE = "item-template";

    public static final String TOPIC_MAPPING = "map";
    private static final String MAP_SUFFIX = "to";

    public static final String MAP_REG_EX_ENABLE = "map.regex.enable";

    public static final String FIELD_MAPPING = "field";
    public static final String FIELDS_SKIP_FAILED_MAPPING_ENABLE =
            "fields.skip.failed.mapping.enable";

    public static final String FIELDS_MAP_NON_SCALAR_VALUES_ENABLE =
            "fields.map.non.scalar.values.enable";

    public static final String FIELDS_EVALUATE_AS_COMMAND_ENABLE =
            "fields.evaluate.as.command.enable";

    public static final String FIELDS_AUTO_COMMAND_MODE_ENABLE = "fields.auto.command.mode.enable";

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

    public static final String RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR =
            "record.key.evaluator.kvp.pairs.separator";

    public static final String RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR =
            "record.key.evaluator.kvp.key-value.separator";

    public static final String RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR =
            "record.value.evaluator.kvp.pairs.separator";

    public static final String RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR =
            "record.value.evaluator.kvp.key-value.separator";

    public static final String RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE =
            "record.key.evaluator.protobuf.message.type";

    public static final String RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE =
            "record.value.evaluator.protobuf.message.type";

    public static final String RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY =
            "record.extraction.error.strategy";

    public static final String RECORD_CONSUME_WITH_ORDER_STRATEGY =
            "record.consume.with.order.strategy";

    public static final String RECORD_CONSUME_WITH_NUM_THREADS = "record.consume.with.num.threads";

    public static final String ENCRYPTION_ENABLE = "encryption.enable";

    public static final String AUTHENTICATION_ENABLE = "authentication.enable";

    public static final String RECORD_CONSUME_FROM = "record.consume.from";

    public static final String CONSUMER_GROUP_MODE = "consumer.group.mode";

    // Kafka consumer specific settings
    public static final String RECORD_CONSUME_WITH_MAX_POLL_RECORDS =
            "record.consume.with." + MAX_POLL_RECORDS_CONFIG;

    public static final String RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS =
            "record.consume.with." + SESSION_TIMEOUT_MS_CONFIG;

    public static final String RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS =
            "record.consume.with." + MAX_POLL_INTERVAL_MS_CONFIG;

    // Prefix for all hidden consumer configs
    private static final String CONNECTOR_PREFIX = "consumer.";
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
    public static final String CONSUMER_RECONNECT_BACKOFF_MS_CONFIG =
            CONNECTOR_PREFIX + RECONNECT_BACKOFF_MS_CONFIG;
    public static final String CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG =
            CONNECTOR_PREFIX + RECONNECT_BACKOFF_MAX_MS_CONFIG;
    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS =
            CONNECTOR_PREFIX + HEARTBEAT_INTERVAL_MS_CONFIG;
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
                        .add(MAP_REG_EX_ENABLE, false, false, BOOL, defaultValue("false"))
                        .add(FIELD_MAPPING, true, true, TEXT)
                        .add(
                                FIELDS_SKIP_FAILED_MAPPING_ENABLE,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(
                                FIELDS_MAP_NON_SCALAR_VALUES_ENABLE,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(
                                RECORD_KEY_EVALUATOR_TYPE,
                                false,
                                false,
                                EVALUATOR,
                                defaultValue(EvaluatorType.STRING.toString()))
                        .add(
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(
                                FIELDS_AUTO_COMMAND_MODE_ENABLE,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
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
                        .add(
                                RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR,
                                false,
                                false,
                                CHAR,
                                defaultValue(","))
                        .add(
                                RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR,
                                false,
                                false,
                                CHAR,
                                defaultValue("="))
                        .add(
                                RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR,
                                false,
                                false,
                                CHAR,
                                defaultValue(","))
                        .add(
                                RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR,
                                false,
                                false,
                                CHAR,
                                defaultValue("="))
                        .add(RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE, false, false, TEXT)
                        .add(RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE, false, false, TEXT)
                        .add(
                                RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY,
                                false,
                                false,
                                ERROR_STRATEGY,
                                defaultValue("IGNORE_AND_CONTINUE"))
                        .add(
                                RECORD_CONSUME_WITH_NUM_THREADS,
                                false,
                                false,
                                THREADS,
                                defaultValue("1"))
                        .add(
                                RECORD_CONSUME_WITH_ORDER_STRATEGY,
                                false,
                                false,
                                ORDER_STRATEGY,
                                defaultValue("ORDER_BY_PARTITION"))
                        .add(ENCRYPTION_ENABLE, false, false, BOOL, defaultValue("false"))
                        .add(AUTHENTICATION_ENABLE, false, false, BOOL, defaultValue("false"))
                        .add(
                                RECORD_CONSUME_FROM,
                                false,
                                false,
                                CONSUME_FROM,
                                defaultValue(RecordConsumeFrom.LATEST.toString()))
                        .add(
                                CONSUMER_GROUP_MODE,
                                false,
                                false,
                                GROUP_MODE,
                                defaultValue(
                                        ConsumerGroupMode.GROUP.toString()))
                        .add(
                                CONSUMER_CLIENT_ID,
                                true,
                                false,
                                BLANKABLE_TEXT,
                                false,
                                defaultValue(
                                        params -> {
                                            String hostList = params.get(BOOTSTRAP_SERVERS);
                                            if (hostList == null) {
                                                return "";
                                            }
                                            if (Split.byComma(hostList).stream()
                                                    .flatMap(s -> Split.asPairWithColon(s).stream())
                                                    .map(p -> p.key())
                                                    .allMatch(
                                                            s -> s.endsWith(".confluent.cloud"))) {
                                                return LIGHTSTREAMER_CLIENT_ID;
                                            }
                                            return "";
                                        }))
                        .add(
                                CONSUMER_ENABLE_AUTO_COMMIT_CONFIG,
                                true,
                                false,
                                BOOL,
                                false,
                                defaultValue("false"))
                        .add(
                                CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG,
                                false,
                                false,
                                NON_NEGATIVE_INT)
                        .add(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG, false, false, NON_NEGATIVE_INT)
                        .add(CONSUMER_FETCH_MIN_BYTES_CONFIG, false, false, NON_NEGATIVE_INT)
                        .add(CONSUMER_FETCH_MAX_BYTES_CONFIG, false, false, NON_NEGATIVE_INT)
                        .add(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG, false, false, NON_NEGATIVE_INT)
                        .add(
                                RECORD_CONSUME_WITH_MAX_POLL_RECORDS,
                                false,
                                false,
                                POSITIVE_INT,
                                true,
                                defaultValue("500"))
                        .add(CONSUMER_HEARTBEAT_INTERVAL_MS, false, false, INT)
                        .add(
                                RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS,
                                false,
                                false,
                                INT,
                                true,
                                defaultValue("45000"))
                        .add(
                                RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS,
                                false,
                                false,
                                POSITIVE_INT,
                                true,
                                defaultValue("30000"))
                        .add(
                                CONSUMER_METADATA_MAX_AGE_CONFIG,
                                true,
                                false,
                                INT,
                                false,
                                defaultValue("250"))
                        .add(
                                CONSUMER_REQUEST_TIMEOUT_MS_CONFIG,
                                true,
                                false,
                                INT,
                                false,
                                defaultValue("30000"))
                        .add(CONSUMER_RETRIES, true, false, INT, false, defaultValue("0"))
                        .add(
                                CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG,
                                true,
                                false,
                                INT,
                                false,
                                defaultValue("60000"))
                        .withEnabledChildConfigs(EncryptionConfigs.spec(), ENCRYPTION_ENABLE)
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

    private ConnectorConfig(ConfigsSpec spec, Map<String, String> configs) throws ConfigException {
        super(spec, configs);
        this.consumerProps = initProps();
        itemTemplateConfigs = ItemTemplateConfigs.from(getValues(ITEM_TEMPLATE));
        topicMappings = TopicMappingConfig.from(getValues(TOPIC_MAPPING));
        fieldConfigs = FieldConfigs.from(getValues(FIELD_MAPPING));
        postValidate();
    }

    public ConnectorConfig(Map<String, String> configs) throws ConfigException {
        this(CONFIG_SPEC, configs);
    }

    @Override
    protected final void postValidate() throws ConfigException {
        checkSchemaConfig(true);
        checkSchemaConfig(false);
        checkTopicMappingRegex();
        checkCommandMode();
    }

    private void checkSchemaConfig(boolean isKey) {
        String schemaPathKey =
                isKey ? RECORD_KEY_EVALUATOR_SCHEMA_PATH : RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
        String evaluatorKey = isKey ? RECORD_KEY_EVALUATOR_TYPE : RECORD_VALUE_EVALUATOR_TYPE;
        String schemaEnabledKey =
                isKey
                        ? RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE
                        : RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
        EvaluatorType evaluatorType = getEvaluator(evaluatorKey);
        if (evaluatorType == EvaluatorType.AVRO || evaluatorType == EvaluatorType.PROTOBUF) {
            if (!getBoolean(schemaEnabledKey)) {
                try {
                    getFile(schemaPathKey, true);
                } catch (ConfigException ce) {
                    throw new ConfigException(
                            "Specify a valid file path for [%s] or set [%s] to true"
                                    .formatted(schemaPathKey, schemaEnabledKey));
                }
                if (EvaluatorType.PROTOBUF.equals(evaluatorType)) {
                    String messageTypeKey =
                            isKey
                                    ? RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE
                                    : RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE;
                    getText(messageTypeKey, true);
                }
                return;
            }
            if (EvaluatorType.PROTOBUF.equals(evaluatorType) && isAzureSchemaRegistryEnabled()) {
                throw new ConfigException(
                        "Schema registry provider [AZURE] does not support Protobuf schema evaluation for record %s"
                                .formatted(isKey ? "key" : "value"));
            }
        }
    }

    private void checkTopicMappingRegex() throws ConfigException {
        if (!isMapRegExEnabled()) {
            return;
        }

        topicMappings.stream()
                .map(TopicMappingConfig::topic)
                .forEach(
                        t -> {
                            try {
                                Pattern.compile(t);
                            } catch (PatternSyntaxException pe) {
                                throw new ConfigException(
                                        "Specify a valid regular expression for parameter [map.%s.to]"
                                                .formatted(t));
                            }
                        });
    }

    private void checkCommandMode() {
        if (isAutoCommandModeEnabled()) {
            checkCommandKey();
            return;
        }

        if (isCommandEnforceEnabled()) {
            if (getRecordConsumeWithNumThreads() != 1) {
                throw new ConfigException(
                        "Command mode requires exactly one consumer thread. Parameter [%s] must be set to [1]"
                                .formatted(RECORD_CONSUME_WITH_NUM_THREADS));
            }
            checkCommandKey();
            if (fieldConfigs.namedFieldsExpressions().get("command") == null) {
                throw new ConfigException(
                        "Command mode requires a command field. Parameter [%s] must be set"
                                .formatted("field.command"));
            }
        }
    }

    private void checkCommandKey() {
        if (fieldConfigs.namedFieldsExpressions().get("key") == null) {
            throw new ConfigException(
                    "Command mode requires a key field. Parameter [%s] must be set"
                            .formatted("field.key"));
        }
    }

    private Properties initProps() {
        NonNullKeyProperties properties = new NonNullKeyProperties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, getHostsList(BOOTSTRAP_SERVERS));
        properties.setProperty(GROUP_ID_CONFIG, getText(GROUP_ID));
        properties.setProperty(CLIENT_ID_CONFIG, get(CONSUMER_CLIENT_ID, BLANKABLE_TEXT, false));
        properties.setProperty(METADATA_MAX_AGE_CONFIG, getInt(CONSUMER_METADATA_MAX_AGE_CONFIG));
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, getRecordConsumeFrom().toPropertyValue());
        properties.setProperty(
                ENABLE_AUTO_COMMIT_CONFIG, getBooleanStr(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG));
        properties.setProperty(
                FETCH_MIN_BYTES_CONFIG, getNonNegativeInt(CONSUMER_FETCH_MIN_BYTES_CONFIG));
        properties.setProperty(
                FETCH_MAX_BYTES_CONFIG, getNonNegativeInt(CONSUMER_FETCH_MAX_BYTES_CONFIG));
        properties.setProperty(
                FETCH_MAX_WAIT_MS_CONFIG, getNonNegativeInt(CONSUMER_FETCH_MAX_WAIT_MS_CONFIG));
        properties.setProperty(
                HEARTBEAT_INTERVAL_MS_CONFIG, getInt(CONSUMER_HEARTBEAT_INTERVAL_MS));
        properties.setProperty(
                MAX_POLL_RECORDS_CONFIG, getPositiveInt(RECORD_CONSUME_WITH_MAX_POLL_RECORDS));
        properties.setProperty(
                RECONNECT_BACKOFF_MAX_MS_CONFIG,
                getNonNegativeInt(CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG));
        properties.setProperty(
                RECONNECT_BACKOFF_MS_CONFIG,
                getNonNegativeInt(CONSUMER_RECONNECT_BACKOFF_MS_CONFIG));
        properties.setProperty(
                SESSION_TIMEOUT_MS_CONFIG, getInt(RECORD_CONSUME_WITH_SESSION_TIMEOUT_MS));
        properties.setProperty(
                MAX_POLL_INTERVAL_MS_CONFIG,
                getPositiveInt(RECORD_CONSUME_WITH_MAX_POLL_INTERVAL_MS));
        properties.setProperty(
                REQUEST_TIMEOUT_MS_CONFIG, getInt(CONSUMER_REQUEST_TIMEOUT_MS_CONFIG));
        properties.setProperty(
                DEFAULT_API_TIMEOUT_MS_CONFIG, getInt(CONSUMER_DEFAULT_API_TIMEOUT_MS_CONFIG));
        properties.setProperty(AdminClientConfig.RETRIES_CONFIG, getInt(CONSUMER_RETRIES));

        properties.putAll(EncryptionConfigs.addEncryption(this));
        properties.putAll(BrokerAuthenticationConfigs.addAuthentication(this));
        properties.putAll(SchemaRegistryConfigs.addSchemaRegistry(this));

        return properties.unmodifiable();
    }

    static ConfigsSpec configSpec() {
        return CONFIG_SPEC;
    }

    public static ConnectorConfig newConfig(File adapterDir, Map<String, String> params)
            throws ConfigException {
        return new ConnectorConfig(
                AbstractConfig.resolveFilePaths(CONFIG_SPEC, params, adapterDir));
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

    public boolean isCommandEnforceEnabled() {
        return getBoolean(FIELDS_EVALUATE_AS_COMMAND_ENABLE);
    }

    public boolean isAutoCommandModeEnabled() {
        return getBoolean(FIELDS_AUTO_COMMAND_MODE_ENABLE);
    }

    public CommandModeStrategy getCommandModeStrategy() {
        return CommandModeStrategy.from(isAutoCommandModeEnabled(), isCommandEnforceEnabled());
    }

    public final RecordConsumeFrom getRecordConsumeFrom() {
        return RecordConsumeFrom.valueOf(get(RECORD_CONSUME_FROM, CONSUME_FROM, false));
    }

    public final RecordErrorHandlingStrategy getRecordExtractionErrorHandlingStrategy() {
        return RecordErrorHandlingStrategy.valueOf(
                get(RECORD_EXTRACTION_ERROR_HANDLING_STRATEGY, ERROR_STRATEGY, false));
    }

    public final RecordConsumeWithOrderStrategy getRecordConsumeWithOrderStrategy() {
        return RecordConsumeWithOrderStrategy.valueOf(
                get(RECORD_CONSUME_WITH_ORDER_STRATEGY, ORDER_STRATEGY, false));
    }

    public final ConsumerGroupMode getConsumerGroupMode() {
        return ConsumerGroupMode.valueOf(
                get(CONSUMER_GROUP_MODE, ConfType.GROUP_MODE, false));
    }

    public final boolean isStandalone() {
        return getConsumerGroupMode() == ConsumerGroupMode.STANDALONE;
    }

    public final int getRecordConsumeWithNumThreads() {
        return Integer.parseInt(getThreads(RECORD_CONSUME_WITH_NUM_THREADS));
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

    public boolean hasSchemaFile() {
        return hasKeySchemaFile() || hasValueSchemaFile();
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

    public boolean isEncryptionEnabled() {
        return getBoolean(ENCRYPTION_ENABLE);
    }

    public boolean isKeystoreEnabled() {
        checkEncryptionEnabled();
        return getBoolean(EncryptionConfigs.ENABLE_MTLS);
    }

    private void checkEncryptionEnabled() {
        if (!isEncryptionEnabled()) {
            throw new ConfigException(
                    "Encryption is not enabled. Check parameter [%s]".formatted(ENCRYPTION_ENABLE));
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

    public boolean isAwsMskIamEnabled() {
        return authenticationMechanism().equals(SaslMechanism.AWS_MSK_IAM);
    }

    private void checkAwsMskIam() {
        if (!isAwsMskIamEnabled()) {
            throw new ConfigException(
                    "AWS_MSK_IAM is not configured. Check parameter [%s]"
                            .formatted(BrokerAuthenticationConfigs.SASL_MECHANISM));
        }
    }

    public String awsMskIamCredentialProfileName() {
        checkAwsMskIam();
        return getText(BrokerAuthenticationConfigs.AWS_MSK_IAM_CREDENTIAL_PROFILE_NAME);
    }

    public String awsMskIamRoleArn() {
        checkAwsMskIam();
        return getText(BrokerAuthenticationConfigs.AWS_MSK_IAM_ROLE_ARN);
    }

    public String awsMskIamRoleSessionName() {
        checkAwsMskIam();
        return getText(BrokerAuthenticationConfigs.AWS_MSK_IAM_ROLE_SESSION_NAME);
    }

    public String awsMskIamStsRegion() {
        checkAwsMskIam();
        return getText(BrokerAuthenticationConfigs.AWS_MSK_IAM_STS_REGION);
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

    private void checkConfluentSchemaRegistryEnabled() {
        if (!isConfluentSchemaRegistryEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not set to [%s]"
                            .formatted(
                                    SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                    SchemaRegistryProvider.CONFLUENT));
        }
    }

    private boolean isConfluentSchemaRegistryEnabled() {
        return SchemaRegistryProvider.CONFLUENT.equals(schemaRegistryProvider());
    }

    private void checkAzureSchemaRegistryEnabled() {
        if (!isAzureSchemaRegistryEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not set to [%s]"
                            .formatted(
                                    SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                                    SchemaRegistryProvider.AZURE));
        }
    }

    private boolean isAzureSchemaRegistryEnabled() {
        return SchemaRegistryProvider.AZURE.equals(schemaRegistryProvider());
    }

    public boolean isConfluentSchemaRegistryEncryptionEnabled() {
        checkConfluentSchemaRegistryEnabled();
        return schemaRegistryUrl().startsWith("https");
    }

    private void checkConfluentSchemaRegistryEncryptionEnabled() {
        checkConfluentSchemaRegistryEnabled();
        if (!isConfluentSchemaRegistryEncryptionEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not set to https protocol"
                            .formatted(SchemaRegistryConfigs.URL));
        }
    }

    public List<SslProtocol> confluentSchemaRegistryEnabledProtocols() {
        return SslProtocol.fromValueStr(confluentSchemaRegistryEnabledProtocolsAsStr());
    }

    public String confluentSchemaRegistryEnabledProtocolsAsStr() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return get(
                SchemaRegistryConfigs.CONFLUENT_SSL_ENABLED_PROTOCOLS,
                ConfType.SSL_ENABLED_PROTOCOLS,
                false);
    }

    public SslProtocol confluentSchemaRegistrySslProtocol() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return SslProtocol.fromName(
                get(SchemaRegistryConfigs.CONFLUENT_SSL_PROTOCOL, ConfType.SSL_PROTOCOL, false));
    }

    public KeystoreType confluentSchemaRegistryTruststoreType() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return KeystoreType.valueOf(
                get(
                        SchemaRegistryConfigs.CONFLUENT_TRUSTSTORE_TYPE,
                        ConfType.KEYSTORE_TYPE,
                        false));
    }

    public String confluentSchemaRegistryTruststorePath() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return getFile(SchemaRegistryConfigs.CONFLUENT_TRUSTSTORE_PATH);
    }

    public String confluentSchemaRegistryTruststorePassword() {
        checkSchemaRegistryEnabled();
        boolean isRequired = confluentSchemaRegistryTruststorePath() != null;
        return getText(SchemaRegistryConfigs.CONFLUENT_TRUSTSTORE_PASSWORD, isRequired);
    }

    public List<String> confluentSchemaRegistryCipherSuites() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return getTextList(SchemaRegistryConfigs.CONFLUENT_SSL_CIPHER_SUITES);
    }

    public String confluentSchemaRegistryCipherSuitesAsStr() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return get(SchemaRegistryConfigs.CONFLUENT_SSL_CIPHER_SUITES, ConfType.TEXT_LIST, false);
    }

    public String confluentSchemaRegistrySslProvider() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return getText(SchemaRegistryConfigs.CONFLUENT_SSL_PROVIDER);
    }

    public boolean isConfluentSchemaRegistryHostNameVerificationEnabled() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return getBoolean(SchemaRegistryConfigs.CONFLUENT_HOSTNAME_VERIFICATION_ENABLE);
    }

    public String schemaRegistryKeyPassword() {
        checkConfluentSchemaRegistryKeystoreEnabled();
        return getText(SchemaRegistryConfigs.CONFLUENT_KEY_PASSWORD);
    }

    public boolean isConfluentSchemaRegistryKeystoreEnabled() {
        checkConfluentSchemaRegistryEncryptionEnabled();
        return getBoolean(SchemaRegistryConfigs.CONFLUENT_KEYSTORE_ENABLE);
    }

    private void checkConfluentSchemaRegistryKeystoreEnabled() {
        if (!isConfluentSchemaRegistryKeystoreEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not enabled"
                            .formatted(SchemaRegistryConfigs.CONFLUENT_KEYSTORE_ENABLE));
        }
    }

    public KeystoreType confluentSchemaRegistryKeystoreType() {
        checkConfluentSchemaRegistryKeystoreEnabled();
        return KeystoreType.valueOf(
                get(SchemaRegistryConfigs.CONFLUENT_KEYSTORE_TYPE, ConfType.KEYSTORE_TYPE, false));
    }

    public String confluentSchemaRegistryKeystorePath() {
        checkConfluentSchemaRegistryKeystoreEnabled();
        return getFile(SchemaRegistryConfigs.CONFLUENT_KEYSTORE_PATH);
    }

    public String confluentSchemaRegistryKeystorePassword() {
        checkConfluentSchemaRegistryKeystoreEnabled();
        return getText(SchemaRegistryConfigs.CONFLUENT_KEYSTORE_PASSWORD);
    }

    public boolean isSchemaRegistryBasicAuthenticationEnabled() {
        checkConfluentSchemaRegistryEnabled();
        return getBoolean(SchemaRegistryConfigs.CONFLUENT_ENABLE_BASIC_AUTHENTICATION);
    }

    private void checkConfluentSchemaRegistryBasicAuthentication() {
        if (!isSchemaRegistryBasicAuthenticationEnabled()) {
            throw new ConfigException(
                    "Parameter [%s] is not enabled"
                            .formatted(
                                    SchemaRegistryConfigs.CONFLUENT_ENABLE_BASIC_AUTHENTICATION));
        }
    }

    public String confluentSchemaRegistryBasicAuthenticationUserName() {
        checkConfluentSchemaRegistryBasicAuthentication();
        return getText(SchemaRegistryConfigs.CONFLUENT_BASIC_AUTHENTICATION_USER_NAME);
    }

    public String confluentSchemaRegistryBasicAuthenticationPassword() {
        checkConfluentSchemaRegistryBasicAuthentication();
        return getText(SchemaRegistryConfigs.CONFLUENT_BASIC_AUTHENTICATION_USER_PASSWORD);
    }

    public SchemaRegistryProvider schemaRegistryProvider() {
        checkSchemaRegistryEnabled();
        return SchemaRegistryProvider.valueOf(
                get(
                        SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER,
                        ConfType.SCHEMA_REGISTRY_PROVIDER,
                        false));
    }

    public String azureTenantId() {
        checkAzureSchemaRegistryEnabled();
        return getText(SchemaRegistryConfigs.AZURE_TENANT_ID);
    }

    public String azureClientId() {
        checkAzureSchemaRegistryEnabled();
        return getText(SchemaRegistryConfigs.AZURE_CLIENT_ID);
    }

    public String azureClientSecret() {
        checkAzureSchemaRegistryEnabled();
        return getText(SchemaRegistryConfigs.AZURE_CLIENT_SECRET);
    }

    public ItemTemplateConfigs getItemTemplateConfigs() {
        return itemTemplateConfigs;
    }

    public List<TopicMappingConfig> getTopicMappings() {
        return topicMappings;
    }

    public boolean isMapRegExEnabled() {
        return getBoolean(MAP_REG_EX_ENABLE);
    }

    public boolean isFieldsSkipFailedMappingEnabled() {
        return getBoolean(FIELDS_SKIP_FAILED_MAPPING_ENABLE);
    }

    public boolean isFieldsMapNonScalarValuesEnabled() {
        return getBoolean(FIELDS_MAP_NON_SCALAR_VALUES_ENABLE);
    }

    public char getKeyKvpPairsSeparator() {
        return get(RECORD_KEY_EVALUATOR_KVP_PAIRS_SEPARATOR, CHAR, false).charAt(0);
    }

    public char getKeyKvpKeyValueSeparator() {
        return get(RECORD_KEY_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, CHAR, false).charAt(0);
    }

    public char getValueKvpPairsSeparator() {
        return get(RECORD_VALUE_EVALUATOR_KVP_PAIRS_SEPARATOR, CHAR, false).charAt(0);
    }

    public char getValueKvpKeyValueSeparator() {
        return get(RECORD_VALUE_EVALUATOR_KVP_KEY_VALUE_SEPARATOR, CHAR, false).charAt(0);
    }

    public String getProtobufKeyMessageType() {
        return getText(RECORD_KEY_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
    }

    public String getProtobufValueMessageType() {
        return getText(RECORD_VALUE_EVALUATOR_PROTOBUF_MESSAGE_TYPE);
    }

    public FieldConfigs getFieldConfigs() {
        return fieldConfigs;
    }
}
