package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.config.ConfigException;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class GenericRecordDeserializerTest {

    @Test
    public void shouldNotDeserializeWithoutSchemaSettings() {
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigs());
        assertThrows(ConfigException.class, () -> new GenericRecordDeserializer(config, true));
        assertThrows(ConfigException.class, () -> new GenericRecordDeserializer(config, false));
    }

    @Test
    public void shouldDeserializeWithSchemaRegistry() {
        Map<String, String> otherConfigs = Map.of(
                ConnectorConfig.KEY_SCHEMA_REGISTRY_URL, "host-key:8080",
                ConnectorConfig.VALUE_SCHEMA_REGISTRY_URL, "host-value:8080");
        ConnectorConfig config = new ConnectorConfig(ConnectorConfigProvider.essentialConfigsWith(otherConfigs));

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaAvroDeserializer.class.getName());
        }

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaAvroDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithLocalSchema() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_SCHEMA_FILE, "value.avsc");
        ConnectorConfig config = new ConnectorConfig(
                ConnectorConfigProvider.essentialConfigsWith(otherConfigs, adapterDir));

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeValueWithLocalSchema() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs = Map.of(ConnectorConfig.VALUE_SCHEMA_FILE, "value.avsc");
        ConnectorConfig config = new ConnectorConfig(
                ConnectorConfigProvider.essentialConfigsWith(otherConfigs, adapterDir));

        try (GenericRecordDeserializer deser = new GenericRecordDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(GenericRecordLocalSchemaDeserializer.class.getName());
        }
    }
}
