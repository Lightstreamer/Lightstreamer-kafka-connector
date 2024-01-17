package com.lightstreamer.kafka_connector.adapter.config;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

        ConfParameter keyConsumerParam = configSpec.getParameter(ConnectorConfig.KEY_CONSUMER);
        assertThat(keyConsumerParam.name()).isEqualTo(ConnectorConfig.KEY_CONSUMER);
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

        ConfParameter valueConsumerParam = configSpec.getParameter(ConnectorConfig.VALUE_CONSUMER);
        assertThat(valueConsumerParam.name()).isEqualTo(ConnectorConfig.VALUE_CONSUMER);
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

        ConfParameter mapParam = configSpec.getParameter(ConnectorConfig.MAP);
        assertThat(mapParam.name()).isEqualTo(ConnectorConfig.MAP);
        assertThat(mapParam.required()).isTrue();
        assertThat(mapParam.multiple()).isTrue();
        assertThat(mapParam.defaultValue()).isNull();
        assertThat(mapParam.type()).isEqualTo(ConfType.Text);

        ConfParameter fieldParam = configSpec.getParameter(ConnectorConfig.FIELD);
        assertThat(fieldParam.name()).isEqualTo(ConnectorConfig.FIELD);
        assertThat(fieldParam.required()).isTrue();
        assertThat(fieldParam.multiple()).isTrue();
        assertThat(fieldParam.defaultValue()).isNull();
        assertThat(fieldParam.type()).isEqualTo(ConfType.Text);

        ConfParameter keySchemaRegistryUrlParam = configSpec.getParameter(ConnectorConfig.KEY_SCHEMA_REGISTRY_URL);
        assertThat(keySchemaRegistryUrlParam.name()).isEqualTo(ConnectorConfig.KEY_SCHEMA_REGISTRY_URL);
        assertThat(keySchemaRegistryUrlParam.required()).isFalse();
        assertThat(keySchemaRegistryUrlParam.multiple()).isFalse();
        assertThat(keySchemaRegistryUrlParam.defaultValue()).isNull();
        assertThat(keySchemaRegistryUrlParam.type()).isEqualTo(ConfType.Host);

        ConfParameter valueSchemaRegistryUrlParam = configSpec.getParameter(ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL);
        assertThat(valueSchemaRegistryUrlParam.name()).isEqualTo(ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL);
        assertThat(valueSchemaRegistryUrlParam.required()).isFalse();
        assertThat(valueSchemaRegistryUrlParam.multiple()).isFalse();
        assertThat(valueSchemaRegistryUrlParam.defaultValue()).isNull();
        assertThat(valueSchemaRegistryUrlParam.type()).isEqualTo(ConfType.Host);
    }

    private Map<String, String> standardParameters() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.ADAPTER_DIR, adapterDir.toString());
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.GROUP_ID, "group-id");
        adapterParams.put(ConnectorConfig.VALUE_CONSUMER, "value-consumer");
        adapterParams.put(ConnectorConfig.VALUE_SCHEMA_FILE, "value-schema-file");
        adapterParams.put(ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL, "value_host:8080");
        adapterParams.put(ConnectorConfig.KEY_CONSUMER, "key-consumer");
        adapterParams.put(ConnectorConfig.KEY_SCHEMA_FILE, "key-schema-file");
        adapterParams.put(ConnectorConfig.KEY_SCHEMA_REGISTRY_URL, "key-host:8080");
        adapterParams.put("map.topic1.to", "item-template1");
        adapterParams.put("field.fieldName1", "bar");
        return adapterParams;
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
        assertThat(config.getText(ConnectorConfig.VALUE_CONSUMER)).isEqualTo("value-consumer");
        assertThat(config.getText(ConnectorConfig.KEY_CONSUMER)).isEqualTo("key-consumer");
    }

    @Test
    public void shouldGetHost() {
        ConnectorConfig config = new ConnectorConfig(standardParameters());
        assertThat(config.getHost(ConnectorConfig.KEY_SCHEMA_REGISTRY_URL)).isEqualTo("key-host:8080");
        assertThat(config.getHost(ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL)).isEqualTo("value_host:8080");
    }

    @Test
    public void shouldGetHostLists() {
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigs());
        assertThat(config.getHostsList(ConnectorConfig.BOOTSTRAP_SERVERS)).isEqualTo("server:8080,server:8081");
    }

    @Test
    public void shouldGetDefaultText() {
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigs());
        assertThat(config.getText(ConnectorConfig.KEY_CONSUMER)).isNotNull();
        assertThat(config.getText(ConnectorConfig.KEY_CONSUMER)).isEqualTo("RAW");

        assertThat(config.getText(ConnectorConfig.VALUE_CONSUMER)).isNotNull();
        assertThat(config.getText(ConnectorConfig.VALUE_CONSUMER)).isEqualTo("RAW");
    }

    @Test
    public void shouldNotGetNonExistingNonRequiredText() {
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigs());
        assertThat(config.getText(ConnectorConfig.KEY_SCHEMA_FILE)).isNull();
        assertThat(config.getText(ConnectorConfig.VALUE_SCHEMA_FILE)).isNull();
    }

    @Test
    public void shouldNotGetNonExistingNonRequiredHost() {
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigs());
        assertThat(config.getHost(ConnectorConfig.KEY_SCHEMA_REGISTRY_URL)).isNull();
        assertThat(config.getHost(ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL)).isNull();
    }

    @Test
    public void shouldGetDirectory() {
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigs(adapterDir));
        assertThat(config.getDirectory(ConnectorConfig.ADAPTER_DIR)).isEqualTo(adapterDir.toString());
    }

}
