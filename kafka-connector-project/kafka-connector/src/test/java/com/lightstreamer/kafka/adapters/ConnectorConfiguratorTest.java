
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

package com.lightstreamer.kafka.adapters;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static com.lightstreamer.kafka.test_utils.ConnectorConfigProvider.minimalConfig;
import static com.lightstreamer.kafka.test_utils.ConnectorConfigProvider.minimalConfigWith;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordDeserializer;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeDeserializer;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;
import com.lightstreamer.kafka.common.mapping.selectors.ValuesExtractor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class ConnectorConfiguratorTest {

    static File adapterDir;

    static ConnectorConfigurator newConfigurator(Map<String, String> params) {
        return new ConnectorConfigurator(params, adapterDir);
    }

    static Map<String, String> basicParameters() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(ConnectorConfig.DATA_ADAPTER_NAME, "CONNECTOR");
        adapterParams.put("item-template.template1", "item1-#{partition=PARTITION}");
        adapterParams.put("map.topic1.to", "item-template.template1");
        adapterParams.put("field.fieldName1", "#{VALUE}");
        return adapterParams;
    }

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir").toFile();
    }

    @Test
    public void shouldConfigureWithBasicParameters() throws IOException {
        ConnectorConfigurator configurator = newConfigurator(basicParameters());
        ConsumerLoopConfig<?, ?> loopConfig = configurator.configure();

        Properties consumerProperties = loopConfig.consumerProperties();
        assertThat(consumerProperties)
                .containsAtLeast(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        "server:8080,server:8081",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "latest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        "false");
        assertThat(consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG))
                .startsWith("KAFKA-CONNECTOR-");

        ValuesExtractor<?, ?> fieldExtractor = loopConfig.fieldsExtractor();
        Schema schema = fieldExtractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1");

        ItemTemplates<?, ?> itemTemplates = loopConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1");
        assertThat(itemTemplates.extractors().map(s -> s.schema().name())).containsExactly("item1");

        assertThat(loopConfig.keyDeserializer().getClass()).isEqualTo(StringDeserializer.class);
        assertThat(loopConfig.valueDeserializer().getClass()).isEqualTo(StringDeserializer.class);
        loopConfig.valueDeserializer();
    }

    @Test
    public void shouldConfigureWithComplexParameters() throws IOException {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put("item-template.template2", "item2-#{key=KEY.attrib}");
        updatedConfigs.put("map.topic1.to", "item-template.template1,item-template.template2");
        updatedConfigs.put("map.topic2.to", "item-template.template1");
        updatedConfigs.put("map.topic3.to", "simple-item1,simple-item2");
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "JSON");
        updatedConfigs.put("field.fieldName1", "#{VALUE.name}");
        updatedConfigs.put("field.fieldName2", "#{VALUE.otherAttrib}");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "JSON");

        ConnectorConfigurator configurator = newConfigurator(updatedConfigs);
        ConsumerLoopConfig<?, ?> loopConfig = configurator.configure();

        ValuesExtractor<?, ?> fieldsExtractor = loopConfig.fieldsExtractor();
        Schema schema = fieldsExtractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1", "fieldName2");

        ItemTemplates<?, ?> itemTemplates = loopConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1", "topic2", "topic3");
        assertThat(itemTemplates.extractors().map(s -> s.schema().name()))
                .containsExactly("item1", "item2", "simple-item1", "simple-item2");

        assertThat(loopConfig.keyDeserializer().getClass()).isEqualTo(JsonNodeDeserializer.class);
        assertThat(loopConfig.valueDeserializer().getClass()).isEqualTo(JsonNodeDeserializer.class);
    }

    @Test
    public void shouldConfigureWithComplexParametersAvro() throws IOException {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put("item-template.template2", "item2-#{key=KEY.attrib}");
        updatedConfigs.put("map.topic1.to", "item-template.template1,item-template.template2");
        updatedConfigs.put("map.topic2.to", "item-template.template1");
        updatedConfigs.put("map.topic3.to", "simple-item1,simple-item2");
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfigs.put("field.fieldName1", "#{VALUE.name}");
        updatedConfigs.put("field.fieldName2", "#{VALUE.otherAttrib}");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfigs.put(SchemaRegistryConfigs.URL, "http://localhost:8081");

        ConnectorConfigurator configurator = newConfigurator(updatedConfigs);
        ConsumerLoopConfig<?, ?> loopConfig = configurator.configure();

        ValuesExtractor<?, ?> fieldsExtractor = loopConfig.fieldsExtractor();
        Schema schema = fieldsExtractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1", "fieldName2");

        ItemTemplates<?, ?> itemTemplates = loopConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1", "topic2", "topic3");
        assertThat(itemTemplates.extractors().map(s -> s.schema().name()))
                .containsExactly("item1", "item2", "simple-item1", "simple-item2");

        assertThat(loopConfig.keyDeserializer().getClass())
                .isEqualTo(GenericRecordDeserializer.class);
        assertThat(loopConfig.valueDeserializer().getClass())
                .isEqualTo(GenericRecordDeserializer.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"map..to", "map.to"})
    public void shouldNotCreateConfiguratorDueToInvalidTopicMappingParameters(
            String topicMappingParam) {
        Map<String, String> config = minimalConfigWith(Map.of(topicMappingParam, "item"));
        ConfigException e =
                assertThrows(
                        ConfigException.class, () -> new ConnectorConfigurator(config, adapterDir));
        assertThat(e.getMessage()).isEqualTo("Specify a valid parameter [map.<...>.to]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"field", "field."})
    public void shouldNotCreateConfiguratorDueToInvalidFieldMappingParameters(
            String fieldMappingParam) {
        Map<String, String> config = minimalConfigWith(Map.of(fieldMappingParam, "field_name"));
        ConfigException ce =
                assertThrows(ConfigException.class, () -> newConfigurator(config).configure());
        assertThat(ce.getMessage()).isEqualTo("Specify a valid parameter [field.<...>]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"item-template", "item-template."})
    public void shouldNotCreateConfiguratorDueToInvalidItemTemplateParameterFormat(
            String itemTemplateParam) {
        Map<String, String> config = minimalConfigWith(Map.of(itemTemplateParam, "field_name"));
        ConfigException ce = assertThrows(ConfigException.class, () -> newConfigurator(config));
        assertThat(ce.getMessage()).isEqualTo("Specify a valid parameter [item-template.<...>]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"value"})
    public void shouldNotCreateConfiguratorDueToInvalidItemTemplateParameter(String template) {
        Map<String, String> config = minimalConfigWith(Map.of("item-template.template", template));
        ConfigException ce = assertThrows(ConfigException.class, () -> newConfigurator(config));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Found the invalid expression [value] while evaluating [template]: <Invalid template expression>");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {","})
    public void shouldNotCreateConfiguratorDueToInvalidItemReference(String itemRef) {
        Map<String, String> configs = new HashMap<>();
        configs.put("map.topic1.to", itemRef);
        Map<String, String> config = minimalConfigWith(configs);
        ConfigException ce = assertThrows(ConfigException.class, () -> newConfigurator(config));

        assertThat(ce.getMessage())
                .isEqualTo("Specify a valid value for parameter [map.topic1.to]");
    }

    @Test
    public void shouldNotConfigureDueToNotExistingItemTemplate() {
        Map<String, String> configs = new HashMap<>();
        configs.put("map.topic1.to", "item-template.no-valid-template");
        Map<String, String> config = minimalConfigWith(configs);
        ConfigException ce =
                assertThrows(ConfigException.class, () -> newConfigurator(config).configure());

        assertThat(ce.getMessage()).isEqualTo("No item template [no-valid-template] found");
    }

    static Stream<Arguments> invalidFieldExpressions() {
        return Stream.of(
                arguments(
                        "NOT_WITHIN_BRACKET_NOTATION",
                        "Found the invalid expression [NOT_WITHIN_BRACKET_NOTATION] while evaluating [fieldName1]: <Invalid field expression>"),
                arguments(
                        "VALUE",
                        "Found the invalid expression [VALUE] while evaluating [fieldName1]: <Invalid field expression>"),
                arguments(
                        "#{UNRECOGNIZED}",
                        "Expected the root token [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC] while evaluating [fieldName1]"));
    }

    @ParameterizedTest
    @MethodSource("invalidFieldExpressions")
    public void shouldNotConfigureDueToInvalidFieldMappingExpressionWithSchema(
            String expression, String expectedErrorMessage) {
        Map<String, String> updatedConfigs = minimalConfig();
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH, "value.avsc");
        updatedConfigs.put("field.fieldName1", expression);

        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () ->
                                new ConnectorConfigurator(
                                                updatedConfigs, new File("src/test/resources"))
                                        .configure());
        assertThat(e.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "a,",
                ".",
                "|",
                "@",
                "item-$",
                "item-#",
                "item-#}",
                "item-#{",
                "item-#{{}",
                "item-#{{a=VALUE}",
                "item-#{}",
                "item-#{{}}",
                "item-#{{a=VALUE}}",
                "item-#{{{}}}",
                "item-#{}}"
            })
    public void shouldNotConfigureDueToInvalidItemTemplateExpression(String expression) {
        Map<String, String> updatedConfigs = minimalConfig();
        updatedConfigs.put("map.topic1.to", "item-template.template1");
        updatedConfigs.put("item-template.template1", expression);

        ConfigException e =
                assertThrows(
                        ConfigException.class, () -> newConfigurator(updatedConfigs).configure());
        assertThat(e.getMessage())
                .isEqualTo(
                        "Found the invalid expression ["
                                + expression
                                + "] while evaluating [template1]: <Invalid template expression>");
    }
}
