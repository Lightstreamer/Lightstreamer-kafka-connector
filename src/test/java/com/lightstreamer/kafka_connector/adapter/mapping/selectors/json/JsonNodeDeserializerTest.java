package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import static com.google.common.truth.Truth.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

public class JsonNodeDeserializerTest {

    @Test
    public void shouldDeserializeWithNoSchema() {
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigs());
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithSchema() {
        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_SCHEMA_REGISTRY_URL, "host-key:8080");
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigsWith(otherConfigs));

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        // No specifi settins for the value deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeValueWithSchema() {
        Map<String, String> otherConfigs = Map.of(ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL, "host-value:8080");
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigsWith(otherConfigs));

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        // No specific settins for the key deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyAndValueWithSchema() {
        Map<String, String> otherConfigs = Map.of(
                ConnectorConfig.KEY_SCHEMA_REGISTRY_URL, "host-value:8080",
                ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL, "host-value:8080");
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigsWith(otherConfigs));

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithLocalSchema() {
        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_SCHEMA_FILE, "schema.json");
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigsWith(otherConfigs));

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }

        // No specifi settins for the value deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }


}
