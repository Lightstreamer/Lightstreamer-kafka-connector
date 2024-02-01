/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.test_utils;

import static com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier.wrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringSelectorSuppliers;
import org.apache.avro.generic.GenericRecord;

public interface SelectorsSuppliers {

  public static SelectorsSupplier<String, String> string() {
    return wrap(
        StringSelectorSuppliers.keySelectorSupplier(),
        StringSelectorSuppliers.valueSelectorSupplier());
  }

  public static SelectorsSupplier<GenericRecord, GenericRecord> avro(ConnectorConfig config) {
    return wrap(
        GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
        GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
  }

  public static SelectorsSupplier<String, GenericRecord> avroValue(ConnectorConfig config) {
    return wrap(
        StringSelectorSuppliers.keySelectorSupplier(),
        GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
  }

  public static SelectorsSupplier<GenericRecord, JsonNode> avroKeyJsonValue(
      ConnectorConfig config) {
    return wrap(
        GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
        JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
  }

  public static SelectorsSupplier<JsonNode, JsonNode> json(ConnectorConfig config) {
    return wrap(
        JsonNodeSelectorsSuppliers.keySelectorSupplier(config),
        JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
  }

  public static SelectorsSupplier<String, JsonNode> jsonValue(ConnectorConfig config) {
    return wrap(
        StringSelectorSuppliers.keySelectorSupplier(),
        JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
  }
}
