package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordDeserializer;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

@Tag("unit")
public class JsonNodeDeserializerTest {

    @Test
    public void shouldDeserializeWithNoSchema() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithSchema() {
        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, "http://host-key:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        // No specific settings for the value deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeValueWithSchema() {
        Map<String, String> otherConfigs = Map.of(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, "http://host-value:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        // No specific settings for the key deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyAndValueWithSchema() {
        Map<String, String> otherConfigs = Map.of(
                ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, "http://host-value:8080",
                ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, "http://host-value:8080");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonSchemaDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyWithLocalSchema() throws IOException {
        Path adapterDir = Files.createTempDirectory("adapter_dir");
        Path keySchemaFile = Files.createTempFile(adapterDir, "key_schema_", "json");
        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_SCHEMA_FILE, keySchemaFile.toFile().getName());
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }

        // No specific settings for the value deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeValueWithLocalSchema() throws IOException {
        Path adapterDir = Files.createTempDirectory("adapter_dir");
        Path valueSchemaFile = Files.createTempFile(adapterDir, "value_schema_", "json");
        Map<String, String> otherConfigs = Map.of(ConnectorConfig.VALUE_SCHEMA_FILE,
                valueSchemaFile.toFile().getName());
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, false)) {
            assertThat(deser.deserializerClassName()).isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }

        // No specific settings for the key deserializer, therefore the
        // KafkaJsonDeserializer is expected here
        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(KafkaJsonDeserializer.class.getName());
        }
    }

    @Test
    public void shouldDeserializeKeyAndValueWithLocalSchema() throws IOException {
        Path adapterDir = Files.createTempDirectory("adapter_dir");
        Path keySchemaFile = Files.createTempFile(adapterDir, "key_schema_", "json");
        Path valueSchemaFile = Files.createTempFile(adapterDir, "value_schema_", "json");
        Map<String, String> otherConfigs = Map.of(
                ConnectorConfig.KEY_SCHEMA_FILE, keySchemaFile.toFile().getName(),
                ConnectorConfig.VALUE_SCHEMA_FILE, valueSchemaFile.toFile().getName());
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }

        try (JsonNodeDeserializer deser = new JsonNodeDeserializer(config, true)) {
            assertThat(deser.deserializerClassName()).isEqualTo(JsonLocalSchemaDeserializer.class.getName());
        }
    }

        @Test
    public void shouldNotDeserializeKeyDueToMissingLocalSchema() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs = Map.of(ConnectorConfig.KEY_SCHEMA_FILE, "no-file.json");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        SerializationException e = assertThrows(SerializationException.class,
                () -> new JsonNodeDeserializer(config, true));
        assertThat(e.getMessage()).isEqualTo("File [" + adapterDir.toAbsolutePath() + "/no-file.json] not found");
    }

    @Test
    public void shouldNotDeserializeValueDueToMissingLocalSchema() throws IOException {
        Path adapterDir = Paths.get("src/test/resources");

        Map<String, String> otherConfigs = Map.of(ConnectorConfig.VALUE_SCHEMA_FILE, "no-file.json");
        ConnectorConfig config = ConnectorConfigProvider.minimalWith(otherConfigs, adapterDir);

        SerializationException e = assertThrows(SerializationException.class,
                () -> new JsonNodeDeserializer(config, false));
        assertThat(e.getMessage()).isEqualTo("File [" + adapterDir.toAbsolutePath() + "/no-file.json] not found");
    }
}