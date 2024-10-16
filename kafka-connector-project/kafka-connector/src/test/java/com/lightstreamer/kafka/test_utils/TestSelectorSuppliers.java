
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

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.AdapterKeyValueSelectorSupplier;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;

import org.apache.avro.generic.GenericRecord;

public interface TestSelectorSuppliers {

    public static KeyValueSelectorSuppliers<String, String> string() {
        return new AdapterKeyValueSelectorSupplier<>(
                OthersSelectorSuppliers.StringKey(), OthersSelectorSuppliers.StringValue());
    }

    public static KeyValueSelectorSuppliers<GenericRecord, GenericRecord> avro(
            ConnectorConfig config) {
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(config);
        return new AdapterKeyValueSelectorSupplier<>(
                g.makeKeySelectorSupplier(), g.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, GenericRecord> avroValue(
            ConnectorConfig config) {
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(config);
        return new AdapterKeyValueSelectorSupplier<>(
                OthersSelectorSuppliers.StringKey(), g.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<GenericRecord, JsonNode> avroKeyJsonValue(
            ConnectorConfig config) {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(config);
        return new AdapterKeyValueSelectorSupplier<>(
                g.makeKeySelectorSupplier(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<JsonNode, JsonNode> json(ConnectorConfig config) {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        return new AdapterKeyValueSelectorSupplier<>(
                j.makeKeySelectorSupplier(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, JsonNode> jsonValue(ConnectorConfig config) {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        return new AdapterKeyValueSelectorSupplier<>(
                OthersSelectorSuppliers.StringKey(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<Object, Object> object() {
        return new ConnectSelectorsSuppliers();
    }
}
