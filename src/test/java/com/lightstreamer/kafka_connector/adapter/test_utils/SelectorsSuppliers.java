package com.lightstreamer.kafka_connector.adapter.test_utils;

import static com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier.wrap;

import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringSelectorSuppliers;

public interface SelectorsSuppliers {

    public static SelectorsSupplier<String, String> string() {
        return wrap(StringSelectorSuppliers.keySelectorSupplier(), StringSelectorSuppliers.valueSelectorSupplier());
    }

    public static SelectorsSupplier<GenericRecord, GenericRecord> genericRecord(ConnectorConfig config) {
        return wrap(GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
                GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static SelectorsSupplier<String, GenericRecord> genericRecordValue(ConnectorConfig config) {
        return wrap(StringSelectorSuppliers.keySelectorSupplier(),
                GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static SelectorsSupplier<GenericRecord, JsonNode> genericRecordKeyJsonNodeValue(ConnectorConfig config) {
        return wrap(GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static SelectorsSupplier<JsonNode, JsonNode> jsonNode(ConnectorConfig config) {
        return wrap(JsonNodeSelectorsSuppliers.keySelectorSupplier(config),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static SelectorsSupplier<String, JsonNode> jsonNodeValue(ConnectorConfig config) {
        return wrap(StringSelectorSuppliers.keySelectorSupplier(),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }
}
