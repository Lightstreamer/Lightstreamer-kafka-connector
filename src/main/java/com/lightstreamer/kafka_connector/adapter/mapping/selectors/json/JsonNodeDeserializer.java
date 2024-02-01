/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.AbstractLocalSchemaDeserializer;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.everit.json.schema.ValidationException;

public class JsonNodeDeserializer implements Deserializer<JsonNode> {

  private Deserializer<JsonNode> deserializer;

  private final boolean isKey;

  JsonNodeDeserializer(ConnectorConfig config, boolean isKey) {
    Map<String, String> props = new HashMap<>();
    this.isKey = isKey;
    if ((isKey && config.hasKeySchemaFile()) || (!isKey && config.hasValueSchemaFile())) {
      deserializer = new JsonLocalSchemaDeserializer(config, isKey);
    } else {
      String schemaRegistryUrl =
          isKey
              ? config.getUrl(ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL, false)
              : config.getUrl(ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL, false);
      if (schemaRegistryUrl != null) {
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_KEY_TYPE, JsonNode.class.getName());
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        deserializer = new KafkaJsonSchemaDeserializer<JsonNode>();
      } else {
        deserializer = new KafkaJsonDeserializer<>();
      }
    }
    deserializer.configure(config.extendsConsumerProps(props), isKey);
  }

  public boolean isKey() {
    return isKey;
  }

  public String deserializerClassName() {
    return deserializer.getClass().getName();
  }

  @Override
  public JsonNode deserialize(String topic, byte[] data) {
    return deserializer.deserialize(topic, data);
  }
}

class JsonLocalSchemaDeserializer extends AbstractLocalSchemaDeserializer<JsonNode> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final KafkaJsonDeserializer<JsonNode> deserializer;

  private JsonSchema schema;

  public JsonLocalSchemaDeserializer(ConnectorConfig config, boolean isKey) {
    super(config, isKey);
    deserializer = new KafkaJsonDeserializer<>();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    deserializer.configure(configs, isKey);
    try {
      schema = new JsonSchema(objectMapper.readTree(schemaFile));
    } catch (IOException e) {
      throw new SerializationException(e.getMessage());
    }
  }

  @Override
  public JsonNode deserialize(String topic, byte[] data) {
    try {
      JsonNode node = deserializer.deserialize(topic, data);
      schema.validate(node);
      return node;
    } catch (IOException | ValidationException e) {
      throw new SerializationException(e.getMessage());
    }
  }
}
