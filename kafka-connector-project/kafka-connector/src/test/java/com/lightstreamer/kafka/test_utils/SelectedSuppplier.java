
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

package com.lightstreamer.kafka.test_utils;

import static com.lightstreamer.kafka.mapping.selectors.Selectors.Selected.with;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.string.StringSelectorSuppliers;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;
import com.lightstreamer.kafka.mapping.selectors.Selectors.Selected;

import org.apache.avro.generic.GenericRecord;

public interface SelectedSuppplier {

    public static Selected<String, String> string() {
        return with(
                StringSelectorSuppliers.keySelectorSupplier(),
                StringSelectorSuppliers.valueSelectorSupplier());
    }

    public static Selected<GenericRecord, GenericRecord> avro(ConnectorConfig config) {
        return with(
                GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
                GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static Selected<String, GenericRecord> avroValue(ConnectorConfig config) {
        return with(
                StringSelectorSuppliers.keySelectorSupplier(),
                GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static Selected<GenericRecord, JsonNode> avroKeyJsonValue(ConnectorConfig config) {
        return with(
                GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static Selected<JsonNode, JsonNode> json(ConnectorConfig config) {
        return with(
                JsonNodeSelectorsSuppliers.keySelectorSupplier(config),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static Selected<String, JsonNode> jsonValue(ConnectorConfig config) {
        return with(
                StringSelectorSuppliers.keySelectorSupplier(),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static Selected<Object, Object> object() {
        return with(
                ConnectSelectorsSuppliers.keySelectorSupplier(),
                ConnectSelectorsSuppliers.valueSelectorSupplier());
    }
}
