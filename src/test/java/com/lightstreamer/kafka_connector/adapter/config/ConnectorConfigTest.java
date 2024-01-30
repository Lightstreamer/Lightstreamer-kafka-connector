package com.lightstreamer.kafka_connector.adapter.config;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;

public class ConnectorConfigTest {

    private Path adapterDir;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir");
    }

    @Test
    public void shouldCreateConfigSpec() {
        ConfigSpec configSpec = ConnectorConfig.configSpec();

        ConfParameter adapterDir = configSpec.getParameter(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDir.name()).isEqualTo(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDir.required()).isTrue();
        assertThat(adapterDir.multiple()).isFalse();
        assertThat(adapterDir.mutable()).isTrue();
        assertThat(adapterDir.defaultValue()).isNull();
        assertThat(adapterDir.type()).isEqualTo(ConfType.Directory);

        ConfParameter adapterConfId = configSpec.getParameter(ConnectorConfig.ADAPTERS_CONF_ID);
        assertThat(adapterConfId.name()).isEqualTo(ConnectorConfig.ADAPTERS_CONF_ID);
        assertThat(adapterConfId.required()).isTrue();
        assertThat(adapterConfId.multiple()).isFalse();
        assertThat(adapterConfId.mutable()).isTrue();
        assertThat(adapterConfId.defaultValue()).isNull();
        assertThat(adapterConfId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter dataAdapterName = configSpec.getParameter(ConnectorConfig.DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.name()).isEqualTo(ConnectorConfig.DATA_ADAPTER_NAME);
        assertThat(dataAdapterName.required()).isTrue();
        assertThat(dataAdapterName.multiple()).isFalse();
        assertThat(dataAdapterName.mutable()).isTrue();
        assertThat(dataAdapterName.defaultValue()).isNull();
        assertThat(dataAdapterName.type()).isEqualTo(ConfType.TEXT);

        ConfParameter bootStrapServers = configSpec.getParameter(ConnectorConfig.BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.name()).isEqualTo(ConnectorConfig.BOOTSTRAP_SERVERS);
        assertThat(bootStrapServers.required()).isTrue();
        assertThat(bootStrapServers.multiple()).isFalse();
        assertThat(bootStrapServers.mutable()).isTrue();
        assertThat(bootStrapServers.defaultValue()).isNull();
        assertThat(bootStrapServers.type()).isEqualTo(ConfType.HostsList);

        ConfParameter groupId = configSpec.getParameter(ConnectorConfig.GROUP_ID);
        assertThat(groupId.name()).isEqualTo(ConnectorConfig.GROUP_ID);
        assertThat(groupId.required()).isFalse();
        assertThat(groupId.multiple()).isFalse();
        assertThat(groupId.mutable()).isTrue();
        assertThat(groupId.defaultValue()).isNotNull();
        assertThat(groupId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyEvaluatorType = configSpec.getParameter(ConnectorConfig.KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.name()).isEqualTo(ConnectorConfig.KEY_EVALUATOR_TYPE);
        assertThat(keyEvaluatorType.required()).isFalse();
        assertThat(keyEvaluatorType.multiple()).isFalse();
        assertThat(keyEvaluatorType.mutable()).isTrue();
        assertThat(keyEvaluatorType.defaultValue()).isEqualTo("RAW");
        assertThat(keyEvaluatorType.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keySchemaFile = configSpec.getParameter(ConnectorConfig.KEY_SCHEMA_FILE);
        assertThat(keySchemaFile.name()).isEqualTo(ConnectorConfig.KEY_SCHEMA_FILE);
        assertThat(keySchemaFile.required()).isFalse();
        assertThat(keySchemaFile.multiple()).isFalse();
        assertThat(keySchemaFile.mutable()).isTrue();
        assertThat(keySchemaFile.defaultValue()).isNull();
        assertThat(keySchemaFile.type()).isEqualTo(ConfType.TEXT);

        ConfParameter valueEvaluatorType = configSpec.getParameter(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.name()).isEqualTo(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        assertThat(valueEvaluatorType.required()).isFalse();
        assertThat(valueEvaluatorType.multiple()).isFalse();
        assertThat(valueEvaluatorType.mutable()).isTrue();
        assertThat(valueEvaluatorType.defaultValue()).isEqualTo("RAW");
        assertThat(valueEvaluatorType.type()).isEqualTo(ConfType.TEXT);

        ConfParameter valueSchemaFile = configSpec.getParameter(ConnectorConfig.VALUE_SCHEMA_FILE);
        assertThat(valueSchemaFile.name()).isEqualTo(ConnectorConfig.VALUE_SCHEMA_FILE);
        assertThat(valueSchemaFile.required()).isFalse();
        assertThat(valueSchemaFile.multiple()).isFalse();
        assertThat(valueSchemaFile.mutable()).isTrue();
        assertThat(valueSchemaFile.defaultValue()).isNull();
        assertThat(valueSchemaFile.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemTemplate = configSpec.getParameter(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.name()).isEqualTo(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.required()).isTrue();
        assertThat(itemTemplate.multiple()).isTrue();
        assertThat(itemTemplate.suffix()).isNull();
        assertThat(itemTemplate.mutable()).isTrue();
        ;
        assertThat(itemTemplate.defaultValue()).isNull();
        assertThat(itemTemplate.type()).isEqualTo(ConfType.TEXT);

        ConfParameter topicMapping = configSpec.getParameter(ConnectorConfig.TOPIC_MAPPING);
        assertThat(topicMapping.name()).isEqualTo(ConnectorConfig.TOPIC_MAPPING);
        assertThat(topicMapping.required()).isTrue();
        assertThat(topicMapping.multiple()).isTrue();
        assertThat(topicMapping.suffix()).isEqualTo("to");
        assertThat(topicMapping.mutable()).isTrue();
        assertThat(topicMapping.defaultValue()).isNull();
        assertThat(topicMapping.type()).isEqualTo(ConfType.TEXT);

        ConfParameter fieldMapping = configSpec.getParameter(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.name()).isEqualTo(ConnectorConfig.FIELD_MAPPING);
        assertThat(fieldMapping.required()).isTrue();
        assertThat(fieldMapping.multiple()).isTrue();
        assertThat(fieldMapping.suffix()).isNull();
        assertThat(fieldMapping.mutable()).isTrue();
        assertThat(fieldMapping.defaultValue()).isNull();
        assertThat(fieldMapping.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyEvaluatorSchemaRegistryUrl = configSpec
                .getParameter(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(keyEvaluatorSchemaRegistryUrl.name()).isEqualTo(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(keyEvaluatorSchemaRegistryUrl.required()).isFalse();
        assertThat(keyEvaluatorSchemaRegistryUrl.multiple()).isFalse();
        assertThat(keyEvaluatorSchemaRegistryUrl.mutable()).isTrue();
        assertThat(keyEvaluatorSchemaRegistryUrl.defaultValue()).isNull();
        assertThat(keyEvaluatorSchemaRegistryUrl.type()).isEqualTo(ConfType.URL);

        ConfParameter valueEvaluatorSchemaRegistryUrl = configSpec
                .getParameter(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(valueEvaluatorSchemaRegistryUrl.name())
                .isEqualTo(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(valueEvaluatorSchemaRegistryUrl.required()).isFalse();
        assertThat(valueEvaluatorSchemaRegistryUrl.multiple()).isFalse();
        assertThat(valueEvaluatorSchemaRegistryUrl.mutable()).isTrue();
        assertThat(valueEvaluatorSchemaRegistryUrl.defaultValue()).isNull();
        assertThat(valueEvaluatorSchemaRegistryUrl.type()).isEqualTo(ConfType.URL);

        ConfParameter itemInfoName = configSpec
                .getParameter(ConnectorConfig.ITEM_INFO_NAME);
        assertThat(itemInfoName.name()).isEqualTo(ConnectorConfig.ITEM_INFO_NAME);
        assertThat(itemInfoName.required()).isFalse();
        assertThat(itemInfoName.multiple()).isFalse();
        assertThat(itemInfoName.mutable()).isTrue();
        assertThat(itemInfoName.defaultValue()).isEqualTo("INFO");
        assertThat(itemInfoName.type()).isEqualTo(ConfType.TEXT);

        ConfParameter itemInfoField = configSpec
                .getParameter(ConnectorConfig.ITEM_INFO_FIELD);
        assertThat(itemInfoField.name()).isEqualTo(ConnectorConfig.ITEM_INFO_FIELD);
        assertThat(itemInfoField.required()).isFalse();
        assertThat(itemInfoField.multiple()).isFalse();
        assertThat(itemInfoField.mutable()).isTrue();
        assertThat(itemInfoField.defaultValue()).isEqualTo("MSG");
        assertThat(itemInfoField.type()).isEqualTo(ConfType.TEXT);

        ConfParameter enableAutoCommit = configSpec
                .getParameter(ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.name()).isEqualTo(ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(enableAutoCommit.required()).isFalse();
        assertThat(enableAutoCommit.multiple()).isFalse();
        assertThat(enableAutoCommit.mutable()).isFalse();
        assertThat(enableAutoCommit.defaultValue()).isEqualTo("false");
        assertThat(enableAutoCommit.type()).isEqualTo(ConfType.BOOL);

        ConfParameter autoOffsetReset = configSpec
                .getParameter(ConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        assertThat(autoOffsetReset.name()).isEqualTo(ConnectorConfig.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        assertThat(autoOffsetReset.required()).isFalse();
        assertThat(autoOffsetReset.multiple()).isFalse();
        assertThat(autoOffsetReset.mutable()).isTrue();
        assertThat(autoOffsetReset.defaultValue()).isEqualTo("latest");
        assertThat(autoOffsetReset.type()).isEqualTo(ConfType.TEXT);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            KEY                      | EXPECTED_INFIX
            map.topic.to             | topic
            map.topicprefix.topic.to | topicprefix.topic
            map.topic                | ''
            pam.topic.to             | ''
            map.map.my.topic.to.to   | map.my.topic.to
            """)
    public void shouldExtractInfixForMap(String key, String expectedInfix) {
        ConfigSpec configSpec = ConnectorConfig.configSpec();
        Optional<String> infix = ConfigSpec.extractInfix(configSpec.getParameter(ConnectorConfig.TOPIC_MAPPING), key);
        if (!expectedInfix.isBlank()) {
            assertThat(infix).hasValue(expectedInfix);
        } else {
            assertThat(infix).isEmpty();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            KEY                      | EXPECTED_INFIX
            field.name               | name
            myfield.name             | ''
            field.my.name            | my.name
            """)
    public void shouldGetInfixForField(String key, String expectedInfix) {
        ConfigSpec configSpec = ConnectorConfig.configSpec();
        Optional<String> infix = ConfigSpec.extractInfix(configSpec.getParameter(ConnectorConfig.FIELD_MAPPING), key);
        if (!expectedInfix.isBlank()) {
            assertThat(infix).hasValue(expectedInfix);
        } else {
            assertThat(infix).isEmpty();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
            KEY                        | EXPECTED_INFIX
            item-template.template1    | template1
            myitem.template1           | ''
            item-template.my.template1 | my.template1
            """)
    public void shouldGetInfixForItemTemplate(String key, String expectedInfix) {
        ConfigSpec configSpec = ConnectorConfig.configSpec();
        Optional<String> infix = ConfigSpec.extractInfix(configSpec.getParameter(ConnectorConfig.ITEM_TEMPLATE), key);
        if (!expectedInfix.isBlank()) {
            assertThat(infix).hasValue(expectedInfix);
        } else {
            assertThat(infix).isEmpty();
        }
    }

    private Map<String, String> standardParameters() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.VALUE_EVALUATOR_TYPE, "value-consumer");
        adapterParams.put(ConnectorConfig.VALUE_SCHEMA_FILE, "value-schema-file");
        adapterParams.put(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, "http://value-host:8080/registry");
        adapterParams.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "key-consumer");
        adapterParams.put(ConnectorConfig.KEY_SCHEMA_FILE, "key-schema-file");
        adapterParams.put(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, "http://key-host:8080/registry");
        adapterParams.put(ConnectorConfig.ITEM_INFO_NAME, "INFO_ITEM");
        adapterParams.put(ConnectorConfig.ITEM_INFO_FIELD, "INFO_FIELD");
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
        adapterParams.put(ConnectorConfig.CONSUMER_FETCH_MAX_BYTES_CONFIG, "100");
        adapterParams.put(ConnectorConfig.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG, "200");
        adapterParams.put(ConnectorConfig.CONSUMER_FETCH_MIN_BYTES_CONFIG, "300");
        adapterParams.put(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG, "400");
        adapterParams.put(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG, "500");
        adapterParams.put(ConnectorConfig.CONSUMER_HEARTBEAT_INTERVAL_MS, "600");
        adapterParams.put(ConnectorConfig.CONSUMER_MAX_POLL_RECORDS, "700");
        adapterParams.put(ConnectorConfig.CONSUMER_SESSION_TIMEOUT_MS, "800");
        adapterParams.put(ConnectorConfig.CONSUMER_MAX_POLL_INTERVAL_MS, "2000"); // Unmodifiable
        adapterParams.put(ConnectorConfig.CONSUMER_METADATA_MAX_AGE_CONFIGS, "250"); // Unmodifiable
        adapterParams.put("item-template.template1", "item1");
        adapterParams.put("item-template.template2", "item2");
        adapterParams.put("map.topic1.to", "template1");
        adapterParams.put("map.topic2.to", "template2");
        adapterParams.put("field.fieldName1", "bar");
        return adapterParams;
    }

    @Test
    public void shouldSpecifyRequiredParams() {
        ConfigException e = assertThrows(ConfigException.class, () -> new ConnectorConfig(Collections.emptyMap()));
        assertThat(e.getMessage())
                .isEqualTo("Missing required parameter [%s]".formatted(ConnectorConfig.DATA_ADAPTER_NAME));

        Map<String, String> params = new HashMap<>();
        params.put(ConnectorConfig.DATA_ADAPTER_NAME, "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ConnectorConfig.DATA_ADAPTER_NAME));

        params.put(ConnectorConfig.DATA_ADAPTER_NAME, "data_provider_name");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Missing required parameter [%s]".formatted(ConnectorConfig.ADAPTERS_CONF_ID));

        params.put(ConnectorConfig.ADAPTERS_CONF_ID, "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ConnectorConfig.ADAPTERS_CONF_ID));

        params.put(ConnectorConfig.ADAPTERS_CONF_ID, "adapters_conf_id");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Missing required parameter [%s]".formatted(ConnectorConfig.BOOTSTRAP_SERVERS));

        params.put(ConnectorConfig.BOOTSTRAP_SERVERS, "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ConnectorConfig.BOOTSTRAP_SERVERS));

        params.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage()).isEqualTo("Specify at least one parameter [item-template.<...>]");

        params.put("item-template.template1", "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage()).isEqualTo("Specify a valid value for parameter [item-template.template1]");

        params.put("item-template.template1", "template");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage()).isEqualTo("Specify at least one parameter [field.<...>]");

        params.put("field.field1", "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [field.field1]");

        params.put("field.field1", "VALUE");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage()).isEqualTo("Missing required parameter [%s]".formatted(ConnectorConfig.ADAPTER_DIR));

        params.put(ConnectorConfig.ADAPTER_DIR, "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ConnectorConfig.ADAPTER_DIR));

        params.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage()).isEqualTo("Specify at least one parameter [map.<...>.to]");

        params.put("map.topic.to", "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [map.topic.to]");

        params.put("map.topic.to", "aTemplate");
        assertDoesNotThrow(() -> new ConnectorConfig(params));
    }

    @Test
    public void shouldRetrieveConfiguration() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        Map<String, String> configuration = config.configuration();
        assertThat(configuration).isNotEmpty();
    }

    @Test
    public void shouldRetrieveBaseConsumerProperties() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        Properties baseConsumerProps = config.baseConsumerProps();
        assertThat(baseConsumerProps).containsAtLeast(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:8080,server:8081",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "100",
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200",
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "300",
                ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "400",
                ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "500",
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "600",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "700",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "800",
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000",
                ConsumerConfig.METADATA_MAX_AGE_CONFIG, "250");
        assertThat(baseConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG)).startsWith("KAFKA-CONNECTOR-");
    }

    @Test
    public void shouldExtendBaseConsumerProperties() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        Map<String, ?> extendedProps = config.extendsConsumerProps(Map.of("new.key", "new.value"));
        assertThat(extendedProps).containsAtLeast(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:8080,server:8081",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                "new.key", "new.value");
        assertThat(extendedProps.get(ConsumerConfig.GROUP_ID_CONFIG).toString()).startsWith("KAFKA-CONNECTOR-");
    }

    @Test
    public void shouldNotModifyEnableAutoCommitConfig() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        assertThat(config.getBoolean(ConnectorConfig.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo("false");
    }

    @Test
    public void shouldGetText() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        assertThat(config.getText(ConnectorConfig.ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getText(ConnectorConfig.DATA_ADAPTER_NAME)).isEqualTo("CONNECTOR");
        assertThat(config.getText(ConnectorConfig.VALUE_EVALUATOR_TYPE)).isEqualTo("value-consumer");
        assertThat(config.getText(ConnectorConfig.KEY_EVALUATOR_TYPE)).isEqualTo("key-consumer");
        assertThat(config.getText(ConnectorConfig.ITEM_INFO_NAME)).isEqualTo("INFO_ITEM");
        assertThat(config.getText(ConnectorConfig.ITEM_INFO_FIELD)).isEqualTo("INFO_FIELD");

        String groupId = config.getText(ConnectorConfig.GROUP_ID);
        assertThat(groupId).startsWith("KAFKA-CONNECTOR-");
        assertThat(groupId.length()).isGreaterThan("KAFKA-CONNECTOR-".length());
    }

    @Test
    public void shouldGetOverridenGroupId() {
        Map<String, String> updatedConfig = new HashMap<>(standardParameters());
        updatedConfig.put(ConnectorConfig.GROUP_ID, "group-id");
        ConnectorConfig config = new ConnectorConfig(updatedConfig);

        assertThat(config.getText(ConnectorConfig.GROUP_ID)).isEqualTo("group-id");
    }

    @Test
    public void shouldGetValues() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        Map<String, String> topics = config.getValues(ConnectorConfig.TOPIC_MAPPING, true);
        assertThat(topics).containsExactly("topic1", "template1", "topic2", "template2");

        Map<String, String> itemTemplates = config.getValues(ConnectorConfig.ITEM_TEMPLATE, true);
        assertThat(itemTemplates).containsExactly("template1", "item1", "template2", "item2");

        Map<String, String> noRemappledItemTemplates = config.getValues(ConnectorConfig.ITEM_TEMPLATE, false);
        assertThat(noRemappledItemTemplates).containsExactly("item-template.template1", "item1",
                "item-template.template2", "item2");
    }

    @Test
    public void shouldGetAsList() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        List<String> values = config.getAsList(ConnectorConfig.TOPIC_MAPPING, e -> e.getKey() + "_" + e.getValue());
        assertThat(values).containsExactly("topic1_template1", "topic2_template2");
    }

    @Test
    public void shouldGetItemTemplateList() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        List<String> values = config.getAsList(ConnectorConfig.ITEM_TEMPLATE, e -> e.getKey() + "_" + e.getValue());
        assertThat(values).containsExactly("template1_item1", "template2_item2");
    }

    @Test
    public void shouldGetUrl() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        assertThat(config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false))
                .isEqualTo("http://key-host:8080/registry");
        assertThat(config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false))
                .isEqualTo("http://value-host:8080/registry");
    }

    @Test
    public void shouldGetRequiredUrl() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        assertThat(config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, true))
                .isEqualTo("http://key-host:8080/registry");
        assertThat(config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, true))
                .isEqualTo("http://value-host:8080/registry");
    }

    @Test
    public void shouldNotGetRequiredHost() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();

        String keySchemaRegistryUrl = config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false);
        assertThat(keySchemaRegistryUrl).isNull();

        String valuechemaRegistryUrl = config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false);
        assertThat(valuechemaRegistryUrl).isNull();

        ConfigException exception = assertThrows(ConfigException.class,
                () -> config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, true));
        assertThat(exception.getMessage()).isEqualTo("Missing required parameter [key.evaluator.schema.registry.url]");

        ConfigException exception2 = assertThrows(ConfigException.class,
                () -> config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, true));
        assertThat(exception2.getMessage())
                .isEqualTo("Missing required parameter [value.evaluator.schema.registry.url]");
    }

    @Test
    public void shouldGetHostLists() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getHostsList(ConnectorConfig.BOOTSTRAP_SERVERS)).isEqualTo("server:8080,server:8081");
    }

    @Test
    public void shouldGetDefaultText() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getText(ConnectorConfig.ADAPTERS_CONF_ID)).isEqualTo("KAFKA");
        assertThat(config.getText(ConnectorConfig.DATA_ADAPTER_NAME)).isEqualTo("CONNECTOR");
        assertThat(config.getText(ConnectorConfig.KEY_EVALUATOR_TYPE)).isEqualTo("RAW");
        assertThat(config.getText(ConnectorConfig.VALUE_EVALUATOR_TYPE)).isEqualTo("RAW");
        assertThat(config.getText(ConnectorConfig.ITEM_INFO_NAME)).isEqualTo("INFO");
        assertThat(config.getText(ConnectorConfig.ITEM_INFO_FIELD)).isEqualTo("MSG");
    }

    @Test
    public void shouldNotGetNonExistingNonRequiredText() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getText(ConnectorConfig.KEY_SCHEMA_FILE)).isNull();
        assertThat(config.getText(ConnectorConfig.VALUE_SCHEMA_FILE)).isNull();
    }

    @Test
    public void shouldNotGetNonExistingNonRequiredHost() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false)).isNull();
        assertThat(config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false)).isNull();
    }

    @Test
    public void shouldGetDirectory() {
        ConnectorConfig config = ConnectorConfigProvider.minimal(adapterDir);
        assertThat(config.getDirectory(ConnectorConfig.ADAPTER_DIR)).isEqualTo(adapterDir.toString());
    }

    @Test
    public void shouldNoGetNonExistinggNonRequiredInt() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_FETCH_MAX_BYTES_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_FETCH_MIN_BYTES_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_HEARTBEAT_INTERVAL_MS)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_MAX_POLL_RECORDS)).isNull();
        assertThat(config.getInt(ConnectorConfig.CONSUMER_SESSION_TIMEOUT_MS)).isNull();
    }

}
