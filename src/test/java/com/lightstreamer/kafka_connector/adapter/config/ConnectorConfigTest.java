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

        ConfParameter adapterDirParam = configSpec.getParameter(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDirParam.name()).isEqualTo(ConnectorConfig.ADAPTER_DIR);
        assertThat(adapterDirParam.required()).isTrue();
        assertThat(adapterDirParam.multiple()).isFalse();
        assertThat(adapterDirParam.defaultValue()).isNull();
        assertThat(adapterDirParam.type()).isEqualTo(ConfType.Directory);

        ConfParameter bootStrapServersParam = configSpec.getParameter(ConnectorConfig.BOOTSTRAP_SERVERS);
        assertThat(bootStrapServersParam.name()).isEqualTo(ConnectorConfig.BOOTSTRAP_SERVERS);
        assertThat(bootStrapServersParam.required()).isTrue();
        assertThat(bootStrapServersParam.multiple()).isFalse();
        assertThat(bootStrapServersParam.defaultValue()).isNull();
        assertThat(bootStrapServersParam.type()).isEqualTo(ConfType.HostsList);

        ConfParameter groupIdParam = configSpec.getParameter(ConnectorConfig.GROUP_ID);
        assertThat(groupIdParam.name()).isEqualTo(ConnectorConfig.GROUP_ID);
        assertThat(groupIdParam.required()).isTrue();
        assertThat(groupIdParam.multiple()).isFalse();
        assertThat(groupIdParam.defaultValue()).isNull();
        assertThat(groupIdParam.type()).isEqualTo(ConfType.Text);

        ConfParameter keyConsumerParam = configSpec.getParameter(ConnectorConfig.KEY_EVALUATOR_TYPE);
        assertThat(keyConsumerParam.name()).isEqualTo(ConnectorConfig.KEY_EVALUATOR_TYPE);
        assertThat(keyConsumerParam.required()).isFalse();
        assertThat(keyConsumerParam.multiple()).isFalse();
        assertThat(keyConsumerParam.defaultValue()).isEqualTo("RAW");
        assertThat(keyConsumerParam.type()).isEqualTo(ConfType.Text);

        ConfParameter keySchemaFile = configSpec.getParameter(ConnectorConfig.KEY_SCHEMA_FILE);
        assertThat(keySchemaFile.name()).isEqualTo(ConnectorConfig.KEY_SCHEMA_FILE);
        assertThat(keySchemaFile.required()).isFalse();
        assertThat(keySchemaFile.multiple()).isFalse();
        assertThat(keySchemaFile.defaultValue()).isNull();
        assertThat(keySchemaFile.type()).isEqualTo(ConfType.Text);

        ConfParameter valueConsumerParam = configSpec.getParameter(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        assertThat(valueConsumerParam.name()).isEqualTo(ConnectorConfig.VALUE_EVALUATOR_TYPE);
        assertThat(valueConsumerParam.required()).isFalse();
        assertThat(valueConsumerParam.multiple()).isFalse();
        assertThat(valueConsumerParam.defaultValue()).isEqualTo("RAW");
        assertThat(valueConsumerParam.type()).isEqualTo(ConfType.Text);

        ConfParameter valueSchemaFile = configSpec.getParameter(ConnectorConfig.VALUE_SCHEMA_FILE);
        assertThat(valueSchemaFile.name()).isEqualTo(ConnectorConfig.VALUE_SCHEMA_FILE);
        assertThat(valueSchemaFile.required()).isFalse();
        assertThat(valueSchemaFile.multiple()).isFalse();
        assertThat(valueSchemaFile.defaultValue()).isNull();
        assertThat(valueSchemaFile.type()).isEqualTo(ConfType.Text);

        ConfParameter itemTemplate = configSpec.getParameter(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.name()).isEqualTo(ConnectorConfig.ITEM_TEMPLATE);
        assertThat(itemTemplate.required()).isTrue();
        assertThat(itemTemplate.multiple()).isTrue();
        assertThat(itemTemplate.suffix()).isNull();
        assertThat(itemTemplate.defaultValue()).isNull();
        assertThat(itemTemplate.type()).isEqualTo(ConfType.Text);

        ConfParameter mapParam = configSpec.getParameter(ConnectorConfig.TOPIC_MAPPING);
        assertThat(mapParam.name()).isEqualTo(ConnectorConfig.TOPIC_MAPPING);
        assertThat(mapParam.required()).isTrue();
        assertThat(mapParam.multiple()).isTrue();
        assertThat(mapParam.suffix()).isEqualTo("to");
        assertThat(mapParam.defaultValue()).isNull();
        assertThat(mapParam.type()).isEqualTo(ConfType.Text);

        ConfParameter fieldParam = configSpec.getParameter(ConnectorConfig.FIELD);
        assertThat(fieldParam.name()).isEqualTo(ConnectorConfig.FIELD);
        assertThat(fieldParam.required()).isTrue();
        assertThat(fieldParam.multiple()).isTrue();
        assertThat(fieldParam.suffix()).isNull();
        assertThat(fieldParam.defaultValue()).isNull();
        assertThat(fieldParam.type()).isEqualTo(ConfType.Text);

        ConfParameter keySchemaRegistryUrlParam = configSpec
                .getParameter(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(keySchemaRegistryUrlParam.name()).isEqualTo(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(keySchemaRegistryUrlParam.required()).isFalse();
        assertThat(keySchemaRegistryUrlParam.multiple()).isFalse();
        assertThat(keySchemaRegistryUrlParam.defaultValue()).isNull();
        assertThat(keySchemaRegistryUrlParam.type()).isEqualTo(ConfType.URL);

        ConfParameter valueSchemaRegistryUrlParam = configSpec
                .getParameter(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(valueSchemaRegistryUrlParam.name()).isEqualTo(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL);
        assertThat(valueSchemaRegistryUrlParam.required()).isFalse();
        assertThat(valueSchemaRegistryUrlParam.multiple()).isFalse();
        assertThat(valueSchemaRegistryUrlParam.defaultValue()).isNull();
        assertThat(valueSchemaRegistryUrlParam.type()).isEqualTo(ConfType.URL);
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
        Optional<String> infix = ConfigSpec.extractInfix(configSpec.getParameter(ConnectorConfig.FIELD), key);
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
        adapterParams.put(ConnectorConfig.GROUP_ID, "group-id");
        adapterParams.put(ConnectorConfig.VALUE_EVALUATOR_TYPE, "value-consumer");
        adapterParams.put(ConnectorConfig.VALUE_SCHEMA_FILE, "value-schema-file");
        adapterParams.put(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, "http://value-host:8080/registry");
        adapterParams.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "key-consumer");
        adapterParams.put(ConnectorConfig.KEY_SCHEMA_FILE, "key-schema-file");
        adapterParams.put(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, "http://key-host:8080/registry");
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
        assertThat(e.getMessage()).isEqualTo("Missing required parameter [%s]".formatted(ConnectorConfig.GROUP_ID));

        Map<String, String> params = new HashMap<>();
        params.put(ConnectorConfig.GROUP_ID, "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ConnectorConfig.GROUP_ID));

        params.put(ConnectorConfig.GROUP_ID, "group-id");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage()).isEqualTo("Specify at least one parameter [field.<...>]");

        params.put("field.field1", "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [field.field1]");

        params.put("field.field1", "VALUE");
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
        assertThat(e.getMessage()).isEqualTo("Missing required parameter [%s]".formatted(ConnectorConfig.ADAPTER_DIR));

        params.put(ConnectorConfig.ADAPTER_DIR, "");
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage())
                .isEqualTo("Specify a valid value for parameter [%s]".formatted(ConnectorConfig.ADAPTER_DIR));

        params.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());
        e = assertThrows(ConfigException.class, () -> new ConnectorConfig(params));
        assertThat(e.getMessage()).isEqualTo("Specify at least one parameter [map.<...>.to]");

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
        assertThat(baseConsumerProps).containsExactly(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:8080,server:8081",
                ConsumerConfig.GROUP_ID_CONFIG, "group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    public void shouldExtendBaseConsumerProperties() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        Map<String, ?> extendedProps = config.extendsConsumerProps(Map.of("new.key", "new.value"));
        assertThat(extendedProps).containsExactly(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:8080,server:8081",
                ConsumerConfig.GROUP_ID_CONFIG, "group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                "new.key", "new.value");
    }

    @Test
    public void shouldGetText() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        assertThat(config.getText(ConnectorConfig.GROUP_ID)).isEqualTo("group-id");
        assertThat(config.getText(ConnectorConfig.VALUE_EVALUATOR_TYPE)).isEqualTo("value-consumer");
        assertThat(config.getText(ConnectorConfig.KEY_EVALUATOR_TYPE)).isEqualTo("key-consumer");
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
        assertThat(config.getText(ConnectorConfig.KEY_EVALUATOR_TYPE)).isNotNull();
        assertThat(config.getText(ConnectorConfig.KEY_EVALUATOR_TYPE)).isEqualTo("RAW");

        assertThat(config.getText(ConnectorConfig.VALUE_EVALUATOR_TYPE)).isNotNull();
        assertThat(config.getText(ConnectorConfig.VALUE_EVALUATOR_TYPE)).isEqualTo("RAW");
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

}
