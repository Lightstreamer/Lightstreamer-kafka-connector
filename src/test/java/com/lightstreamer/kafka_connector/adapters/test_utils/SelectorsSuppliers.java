
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

package com.lightstreamer.kafka_connector.adapters.test_utils;

import static com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors.SelectorsSupplier.wrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.string.StringSelectorSuppliers;

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
