
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
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.BOOTSTRAP_SERVERS;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.DATA_ADAPTER_NAME;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.FIELDS_EVALUATE_AS_COMMAND_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_CONSUME_WITH_NUM_THREADS;
import static com.lightstreamer.kafka.test_utils.ConnectorConfigProvider.minimalConfig;
import static com.lightstreamer.kafka.test_utils.ConnectorConfigProvider.minimalConfigWith;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandModeStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger.Concurrency;
import com.lightstreamer.kafka.adapters.consumers.trigger.ConsumerTrigger.ConsumerTriggerConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers.KeyValueDeserializers;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.Schema;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

public class ConnectorConfiguratorTest {

    static File ADAPTER_DIR = new File("src/test/resources");

    static ConnectorConfigurator newConfigurator(Map<String, String> params) {
        return new ConnectorConfigurator(params, ADAPTER_DIR);
    }

    static Map<String, String> basicParameters() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.ADAPTERS_CONF_ID, "KAFKA");
        adapterParams.put(DATA_ADAPTER_NAME, "CONNECTOR");
        adapterParams.put("item-template.template1", "item1-#{partition=PARTITION}");
        adapterParams.put("map.topic1.to", "item-template.template1");
        adapterParams.put("field.fieldName1", "#{VALUE}");
        return adapterParams;
    }

    static Stream<Arguments> getMakersArguments() {
        return Stream.of(
                Arguments.of(
                        "LONG",
                        Serdes.Long().deserializer().getClass(),
                        "STRING",
                        Serdes.String().deserializer().getClass()),
                Arguments.of(
                        "INTEGER",
                        Serdes.Integer().deserializer().getClass(),
                        "JSON",
                        KafkaJsonDeserializer.class),
                Arguments.of(
                        "INTEGER",
                        Serdes.Integer().deserializer().getClass(),
                        "PROTOBUF",
                        KafkaProtobufDeserializer.class),
                Arguments.of(
                        "INTEGER",
                        Serdes.Integer().deserializer().getClass(),
                        "KVP",
                        Serdes.String().deserializer().getClass()),
                Arguments.of(
                        "FLOAT",
                        Serdes.Float().deserializer().getClass(),
                        "BOOLEAN",
                        BooleanDeserializer.class),
                Arguments.of(
                        "SHORT",
                        Serdes.Short().deserializer().getClass(),
                        "UUID",
                        Serdes.UUID().deserializer().getClass()),
                Arguments.of(
                        "DOUBLE",
                        Serdes.Double().deserializer().getClass(),
                        "BYTE_ARRAY",
                        Serdes.ByteArray().deserializer().getClass()),
                Arguments.of(
                        "PROTOBUF",
                        KafkaProtobufDeserializer.class,
                        "BYTE_ARRAY",
                        Serdes.ByteArray().deserializer().getClass()),
                Arguments.of(
                        "BYTES",
                        Serdes.Bytes().deserializer().getClass(),
                        "BYTE_BUFFER",
                        Serdes.ByteBuffer().deserializer().getClass()));
    }

    @ParameterizedTest
    @MethodSource("getMakersArguments")
    public void shouldGetMakers(
            String keyType,
            Class<?> expectedKeyDeserializer,
            String valueType,
            Class<?> expectedValueDeserializer) {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, keyType);
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, valueType);
        if (keyType.equals("PROTOBUF")) {
            updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
            updatedConfigs.put(SchemaRegistryConfigs.URL, "http://localhost:8081");
        }
        if (valueType.equals("PROTOBUF")) {
            updatedConfigs.put(
                    ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
            updatedConfigs.put(SchemaRegistryConfigs.URL, "http://localhost:8081");
        }

        ConnectorConfig config = ConnectorConfig.newConfig(ADAPTER_DIR, updatedConfigs);
        WrapperKeyValueSelectorSuppliers<?, ?> wrapper =
                ConnectorConfigurator.mkKeyValueSelectorSuppliers(config);
        KeyValueDeserializers<?, ?> deserializers = wrapper.deserializers();
        assertThat(deserializers.keyDeserializer().getClass()).isEqualTo(expectedKeyDeserializer);
        assertThat(deserializers.valueDeserializer().getClass())
                .isEqualTo(expectedValueDeserializer);
    }

    @Test
    public void shouldConfigureWithBasicParameters() throws IOException {
        ConnectorConfigurator configurator = newConfigurator(basicParameters());
        ConsumerTriggerConfig<?, ?> consumerTriggerConfig = configurator.configure();

        Properties consumerProperties = consumerTriggerConfig.consumerProperties();
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

        DataExtractor<?, ?> fieldExtractor = consumerTriggerConfig.fieldsExtractor();
        assertThat(fieldExtractor.skipOnFailure()).isFalse();
        assertThat(fieldExtractor.mapNonScalars()).isFalse();

        Schema schema = fieldExtractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1");

        ItemTemplates<?, ?> itemTemplates = consumerTriggerConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1");

        Set<Schema> schemas = itemTemplates.getExtractorSchemasByTopicName("topic1");
        assertThat(schemas.stream().map(Schema::name)).containsExactly("item1");

        KeyValueDeserializers<?, ?> deserializers = consumerTriggerConfig.deserializers();
        assertThat(deserializers.keyDeserializer().getClass()).isEqualTo(StringDeserializer.class);
        assertThat(consumerTriggerConfig.deserializers().valueDeserializer().getClass())
                .isEqualTo(StringDeserializer.class);

        assertThat(consumerTriggerConfig.errorHandlingStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE);
        assertThat(consumerTriggerConfig.commandModeStrategy()).isEqualTo(CommandModeStrategy.NONE);

        Concurrency concurrency = consumerTriggerConfig.concurrency();
        assertThat(concurrency.threads()).isEqualTo(1);
        assertThat(concurrency.orderStrategy())
                .isEqualTo(RecordConsumeWithOrderStrategy.ORDER_BY_PARTITION);
        assertThat(concurrency.isParallel()).isFalse();

        assertThat(configurator.getConfig().consumeAtStartup()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 4})
    public void shouldConfigureWithComplexParameters(int threads) throws IOException {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put("item-template.template2", "item2-#{key=KEY}");
        updatedConfigs.put("map.topic1.to", "item-template.template1,item-template.template2");
        updatedConfigs.put("map.topic2.to", "item-template.template1");
        updatedConfigs.put("map.topic3.to", "simple-item1,simple-item2");
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "STRING");
        updatedConfigs.put(ConnectorConfig.FIELDS_TRANSFORM_TO_COMMAND_ENABLE, "true");
        updatedConfigs.put("field.fieldName1", "#{VALUE.name}");
        updatedConfigs.put("field.fieldName2", "#{VALUE.otherAttrib}");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "JSON");
        updatedConfigs.put(
                ConnectorConfig.RECORD_CONSUME_WITH_NUM_THREADS, String.valueOf(threads));
        updatedConfigs.put(ConnectorConfig.RECORD_CONSUME_WITH_ORDER_STRATEGY, "UNORDERED");
        updatedConfigs.put(ConnectorConfig.FIELDS_SKIP_FAILED_MAPPING_ENABLE, "true");
        updatedConfigs.put(ConnectorConfig.FIELDS_MAP_NON_SCALAR_VALUES_ENABLE, "true");
        updatedConfigs.put(ConnectorConfig.RECORD_CONSUME_AT_CONNECTOR_STARTUP, "true");

        ConnectorConfigurator configurator = newConfigurator(updatedConfigs);
        ConsumerTriggerConfig<?, ?> consumerTriggerConfig = configurator.configure();

        DataExtractor<?, ?> fieldsExtractor = consumerTriggerConfig.fieldsExtractor();
        assertThat(fieldsExtractor.skipOnFailure()).isTrue();
        assertThat(fieldsExtractor.mapNonScalars()).isTrue();

        Schema schema = fieldsExtractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1", "fieldName2");

        ItemTemplates<?, ?> itemTemplates = consumerTriggerConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1", "topic2", "topic3");

        Set<Schema> schemasForTopic1 = itemTemplates.getExtractorSchemasByTopicName("topic1");
        assertThat(schemasForTopic1.stream().map(Schema::name)).containsExactly("item1", "item2");

        Set<Schema> schemasForTopic2 = itemTemplates.getExtractorSchemasByTopicName("topic2");
        assertThat(schemasForTopic2.stream().map(Schema::name)).containsExactly("item1");

        Set<Schema> schemasForTopic3 = itemTemplates.getExtractorSchemasByTopicName("topic3");
        assertThat(schemasForTopic3.stream().map(Schema::name))
                .containsExactly("simple-item1", "simple-item2");

        KeyValueDeserializers<?, ?> deserializers = consumerTriggerConfig.deserializers();
        assertThat(deserializers.keyDeserializer().getClass()).isEqualTo(StringDeserializer.class);
        assertThat(consumerTriggerConfig.deserializers().valueDeserializer().getClass())
                .isEqualTo(KafkaJsonDeserializer.class);

        assertThat(consumerTriggerConfig.errorHandlingStrategy())
                .isEqualTo(RecordErrorHandlingStrategy.IGNORE_AND_CONTINUE);
        assertThat(consumerTriggerConfig.commandModeStrategy())
                .isEqualTo(CommandModeStrategy.TRANSFORM);

        Concurrency concurrency = consumerTriggerConfig.concurrency();
        assertThat(concurrency.threads()).isEqualTo(threads);
        assertThat(concurrency.orderStrategy()).isEqualTo(RecordConsumeWithOrderStrategy.UNORDERED);
        assertThat(concurrency.isParallel()).isTrue();

        assertThat(configurator.getConfig().consumeAtStartup()).isTrue();
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
        updatedConfigs.put(ConnectorConfig.FIELDS_EVALUATE_AS_COMMAND_ENABLE, "true");
        updatedConfigs.put("field.key", "#{VALUE.key}");
        updatedConfigs.put("field.command", "#{VALUE.command}");
        updatedConfigs.put("field.fieldName1", "#{VALUE.name}");
        updatedConfigs.put("field.fieldName2", "#{VALUE.otherAttrib}");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "AVRO");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH, "test_schema.avsc");
        updatedConfigs.put(SchemaRegistryConfigs.URL, "http://localhost:8081");

        ConnectorConfigurator configurator = newConfigurator(updatedConfigs);
        ConsumerTriggerConfig<?, ?> consumerTriggerConfig = configurator.configure();

        DataExtractor<?, ?> fieldsExtractor = consumerTriggerConfig.fieldsExtractor();
        Schema schema = fieldsExtractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1", "fieldName2", "key", "command");

        ItemTemplates<?, ?> itemTemplates = consumerTriggerConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1", "topic2", "topic3");

        Set<Schema> schemasForTopic1 = itemTemplates.getExtractorSchemasByTopicName("topic1");
        assertThat(schemasForTopic1.stream().map(Schema::name)).containsExactly("item1", "item2");

        Set<Schema> schemasForTopic2 = itemTemplates.getExtractorSchemasByTopicName("topic2");
        assertThat(schemasForTopic2.stream().map(Schema::name)).containsExactly("item1");

        Set<Schema> schemasForTopic3 = itemTemplates.getExtractorSchemasByTopicName("topic3");
        assertThat(schemasForTopic3.stream().map(Schema::name))
                .containsExactly("simple-item1", "simple-item2");

        KeyValueDeserializers<?, ?> deserializers = consumerTriggerConfig.deserializers();
        assertThat(deserializers.keyDeserializer().getClass().getSimpleName())
                .isEqualTo("WrapperKafkaAvroDeserializer");
        assertThat(
                        consumerTriggerConfig
                                .deserializers()
                                .valueDeserializer()
                                .getClass()
                                .getSimpleName())
                .isEqualTo("GenericRecordLocalSchemaDeserializer");

        assertThat(consumerTriggerConfig.commandModeStrategy())
                .isEqualTo(CommandModeStrategy.ENFORCE);
    }

    @Test
    public void shouldConfigureWithComplexParametersProtoBuf() throws IOException {
        Map<String, String> updatedConfigs = new HashMap<>(basicParameters());
        updatedConfigs.put("item-template.template2", "item2-#{key=KEY.attrib}");
        updatedConfigs.put("map.topic1.to", "item-template.template1,item-template.template2");
        updatedConfigs.put("map.topic2.to", "item-template.template1");
        updatedConfigs.put("map.topic3.to", "simple-item1,simple-item2");
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE, "PROTOBUF");
        updatedConfigs.put(ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfigs.put("field.fieldName1", "#{VALUE.name}");
        updatedConfigs.put("field.fieldName2", "#{VALUE.otherAttrib}");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE, "PROTOBUF");
        updatedConfigs.put(ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE, "true");
        updatedConfigs.put(SchemaRegistryConfigs.URL, "http://localhost:8081");

        ConnectorConfigurator configurator = newConfigurator(updatedConfigs);
        ConsumerTriggerConfig<?, ?> consumerTriggerConfig = configurator.configure();

        DataExtractor<?, ?> fieldsExtractor = consumerTriggerConfig.fieldsExtractor();
        Schema schema = fieldsExtractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1", "fieldName2");

        ItemTemplates<?, ?> itemTemplates = consumerTriggerConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1", "topic2", "topic3");

        Set<Schema> schemasForTopic1 = itemTemplates.getExtractorSchemasByTopicName("topic1");
        assertThat(schemasForTopic1.stream().map(Schema::name)).containsExactly("item1", "item2");

        Set<Schema> schemasForTopic2 = itemTemplates.getExtractorSchemasByTopicName("topic2");
        assertThat(schemasForTopic2.stream().map(Schema::name)).containsExactly("item1");

        Set<Schema> schemasForTopic3 = itemTemplates.getExtractorSchemasByTopicName("topic3");
        assertThat(schemasForTopic3.stream().map(Schema::name))
                .containsExactly("simple-item1", "simple-item2");

        KeyValueDeserializers<?, ?> deserializers = consumerTriggerConfig.deserializers();
        assertThat(deserializers.keyDeserializer().getClass().getSimpleName())
                .isEqualTo("KafkaProtobufDeserializer");
        assertThat(
                        consumerTriggerConfig
                                .deserializers()
                                .valueDeserializer()
                                .getClass()
                                .getSimpleName())
                .isEqualTo("KafkaProtobufDeserializer");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 2})
    public void shouldNotCreateDueToIncompatibleCommandModeAndParallelism(int threads) {
        Map<String, String> config =
                minimalConfigWith(
                        Map.of(
                                FIELDS_EVALUATE_AS_COMMAND_ENABLE,
                                "true",
                                RECORD_CONSUME_WITH_NUM_THREADS,
                                String.valueOf(threads)));
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () -> new ConnectorConfigurator(config, ADAPTER_DIR));
        assertThat(e.getMessage())
                .isEqualTo(
                        "Command mode requires exactly one consumer thread. Parameter [record.consume.with.num.threads] must be set to [1]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"map..to", "map.to"})
    public void shouldNotCreateConfiguratorDueToInvalidTopicMappingParameters(
            String topicMappingParam) {
        Map<String, String> config = minimalConfigWith(Map.of(topicMappingParam, "item"));
        ConfigException e =
                assertThrows(
                        ConfigException.class,
                        () -> new ConnectorConfigurator(config, ADAPTER_DIR));
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

    @Test
    public void shouldNotCreateConfiguratorDueToOrderStrategy() {
        Map<String, String> config =
                minimalConfigWith(
                        Map.of("record.consume.with.order.strategy", "invalidOrderStrategy"));
        ConfigException ce = assertThrows(ConfigException.class, () -> newConfigurator(config));
        assertThat(ce.getMessage())
                .isEqualTo(
                        "Specify a valid value for parameter [record.consume.with.order.strategy]");
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
                        "Found the invalid expression [NOT_WITHIN_BRACKET_NOTATION] while evaluating [fieldName1]: <Invalid expression>"),
                arguments(
                        "VALUE",
                        "Found the invalid expression [VALUE] while evaluating [fieldName1]: <Invalid expression>"),
                arguments(
                        "#{UNRECOGNIZED}",
                        "Found the invalid expression [#{UNRECOGNIZED}] while evaluating [fieldName1]: <Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]>"));
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
