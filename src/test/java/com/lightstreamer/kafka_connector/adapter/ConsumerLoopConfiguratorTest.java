package com.lightstreamer.kafka_connector.adapter;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.lightstreamer.kafka_connector.adapter.ConsumerLoopConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.config.ConfigException;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.Fields.FieldMappings;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeDeserializer;

public class ConsumerLoopConfiguratorTest {

    private static File adapterDir;

    @BeforeEach
    void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir").toFile();
    }

    static ConnectorConfig cgf(Map<String, String> params) {
        return ConnectorConfig.newConfig(adapterDir, params);
    }

    private Map<String, String> basicParameters() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
        adapterParams.put("item-template.template1", "item1-#{}");
        adapterParams.put("map.topic1.to", "item-template.template1");
        adapterParams.put("field.fieldName1", "#{VALUE}");
        return adapterParams;
    }

    @Test
    public void shouldNotConfigureAvroDueToMissingSchemaRegistry() {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "AVRO");

        ConfigException e = assertThrows(ConfigException.class,
                () -> ConsumerLoopConfigurator.configure(cgf(updatedConfigs)));
        assertThat(e.getMessage()).isEqualTo("Missing required parameter [key.evaluator.schema.registry.url]");
    }

    @Test
    public void shouldConfigureAvroWithSchemaRegistry() {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, "http://schema-registry");

        assertDoesNotThrow(() -> ConsumerLoopConfigurator.configure(cgf(updatedConfigs)));
    }

    @Test
    public void shouldShouldNotConfigureAvroDueToMissingLocalSchemaFile() {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.KEY_SCHEMA_FILE, "value.avsc");

        ConfigException e = assertThrows(ConfigException.class,
                () -> ConsumerLoopConfigurator.configure(cgf(updatedConfigs)));
        assertThat(e.getMessage()).isEqualTo("File [%s/%s] not found".formatted(adapterDir, "value.avsc"));
    }

    @Test
    public void shouldConfigureAvroWithLocalSchemaFile() {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.KEY_SCHEMA_FILE, "value.avsc");

        ConnectorConfig cfg = ConnectorConfig.newConfig(new File("src/test/resources"), updatedConfigs);
        assertDoesNotThrow(() -> ConsumerLoopConfigurator.configure(cfg));
    }

    @Test
    public void shouldShouldNotConfigureJsonDueToMissingLocalSchemaFile() {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "JSON");
        updatedConfigs.put(ConnectorConfig.KEY_SCHEMA_FILE, "flights.json");

        ConfigException e = assertThrows(ConfigException.class,
                () -> ConsumerLoopConfigurator.configure(cgf(updatedConfigs)));
        assertThat(e.getMessage()).isEqualTo("File [%s/%s] not found".formatted(adapterDir, "flights.json"));
    }

    @Test
    public void shouldNotConfigureDueToInvalidTemplateReference() {
        Map<String, String> updatedConfigs = basicParameters();
        updatedConfigs.put("map.topic1.to", "no-valid-item-template");

        ConfigException e = assertThrows(ConfigException.class,
                () -> ConsumerLoopConfigurator.configure(cgf(updatedConfigs)));
        assertThat(e.getMessage()).isEqualTo("No item template [no-valid-item-template] found");
    }

    @Test
    public void shouldNotConfigureDueToInvalidFieldMappingExpression() {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put("field.fieldName1", "VALUE");

        ConfigException e = assertThrows(ConfigException.class,
                () -> ConsumerLoopConfigurator.configure(cgf(updatedConfigs)));
        assertThat(e.getMessage())
                .isEqualTo("Found the invalid expression [VALUE] while evaluating [field.fieldName1]");
    }

    @ParameterizedTest
    @ValueSource(strings = { "VALUE", "#{UNRECOGNIZED}" })
    public void shouldNotConfigureDueToInvalidFieldMappingExpressionWithSchema(String expression) {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.KEY_SCHEMA_FILE, "value.avsc");
        updatedConfigs.put("field.fieldName1", expression);

        ConnectorConfig cfg = ConnectorConfig.newConfig(new File("src/test/resources"), updatedConfigs);
        ConfigException e = assertThrows(ConfigException.class, () -> ConsumerLoopConfigurator.configure(cfg));
        assertThat(e.getMessage())
                .isEqualTo("Found the invalid expression [" + expression + "] while evaluating [field.fieldName1]");
    }

    @ParameterizedTest
    @ValueSource(strings = { "a,", ".", "|", "@", "item-$", "item-#{", "item-#{}}" })
    public void shouldNotConfigureDueToInvalidItemTemplateExpression(String expression) {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put("item-template.template1", expression);

        ConfigException e = assertThrows(ConfigException.class,
                () -> ConsumerLoopConfigurator.configure(cgf(updatedConfigs)));
        assertThat(e.getMessage()).isEqualTo("Found the invalid expression [" + expression
                + "] while evaluating [item-template.template1]: <Invalid item>");
    }

    @Test
    public void shouldConfigureWithBasicParameters() throws IOException {
        ConsumerLoopConfig<?, ?> loopConfig = ConsumerLoopConfigurator.configure(cgf(basicParameters()));

        Properties consumerProperties = loopConfig.consumerProperties();
        assertThat(consumerProperties).containsAtLeast(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:8080,server:8081",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        assertThat(consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG)).startsWith("KAFKA-CONNECTOR-");

        FieldMappings<?, ?> fieldMappings = loopConfig.fieldMappings();
        Schema schema = fieldMappings.selectors().schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1");

        ItemTemplates<?, ?> itemTemplates = loopConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1");
        assertThat(itemTemplates.selectors().map(s -> s.schema().name())).containsExactly("item1");

        assertThat(loopConfig.keyDeserializer().getClass()).isEqualTo(StringDeserializer.class);
        assertThat(loopConfig.valueDeserializer().getClass()).isEqualTo(StringDeserializer.class);
        loopConfig.valueDeserializer();
    }

    @Test
    public void shouldConfigureWithComplexParameters() throws IOException {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put("item-template.template2", "item2");
        updatedConfigs.put("map.topic1.to", "item-template.template1,item-template.template2");
        updatedConfigs.put("map.topic2.to", "item-template.template1");
        updatedConfigs.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "JSON");
        updatedConfigs.put("field.fieldName1", "#{VALUE.name}");
        updatedConfigs.put("field.fieldName2", "#{VALUE.otherAttrib}");
        updatedConfigs.put(ConnectorConfig.VALUE_EVALUATOR_TYPE, "JSON");

        ConsumerLoopConfig<?, ?> loopConfig = ConsumerLoopConfigurator.configure(cgf(updatedConfigs));

        FieldMappings<?, ?> fieldMappings = loopConfig.fieldMappings();
        Schema schema = fieldMappings.selectors().schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1", "fieldName2");

        ItemTemplates<?, ?> itemTemplates = loopConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1", "topic2");
        assertThat(itemTemplates.selectors().map(s -> s.schema().name())).containsExactly("item1", "item2");

        assertThat(loopConfig.keyDeserializer().getClass()).isEqualTo(JsonNodeDeserializer.class);
        assertThat(loopConfig.valueDeserializer().getClass()).isEqualTo(JsonNodeDeserializer.class);
        loopConfig.valueDeserializer();
    }

}
