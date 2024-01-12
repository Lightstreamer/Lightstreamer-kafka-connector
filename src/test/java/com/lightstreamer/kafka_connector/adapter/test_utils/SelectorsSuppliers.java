package com.lightstreamer.kafka_connector.adapter.test_utils;

import static com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier.wrap;

import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringSelectorSuppliers;

public interface SelectorsSuppliers {

    public static SelectorsSupplier<String, String> string() {
        return wrap(StringSelectorSuppliers.keySelectorSupplier(), StringSelectorSuppliers.valueSelectorSupplier());
    }

    public static SelectorsSupplier<GenericRecord, GenericRecord> genericRecord() {
        return wrap(GenericRecordSelectorsSuppliers.keySelectorSupplier(),
                GenericRecordSelectorsSuppliers.valueSelectorSupplier());
    }

    public static SelectorsSupplier<JsonNode, JsonNode> jsonNode() {
        return wrap(JsonNodeSelectorsSuppliers.keySelectorSupplier(),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier());
    }
}
