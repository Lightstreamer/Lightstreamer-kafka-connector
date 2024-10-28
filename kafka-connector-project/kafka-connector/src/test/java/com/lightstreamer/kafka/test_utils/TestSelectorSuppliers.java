
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
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;

import org.apache.avro.generic.GenericRecord;

public interface TestSelectorSuppliers {

    public static KeyValueSelectorSuppliers<GenericRecord, GenericRecord> Avro(
            ConnectorConfig config) {
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(config);
        return new WrapperKeyValueSelectorSuppliers<>(
                g.makeKeySelectorSupplier(), g.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, GenericRecord> AvroValue(
            ConnectorConfig config) {
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(config);
        return new WrapperKeyValueSelectorSuppliers<>(
                OthersSelectorSuppliers.StringKey(), g.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<GenericRecord, JsonNode> AvroKeyJsonValue(
            ConnectorConfig config) {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(config);
        return new WrapperKeyValueSelectorSuppliers<>(
                g.makeKeySelectorSupplier(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<JsonNode, JsonNode> Json(ConnectorConfig config) {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        return new WrapperKeyValueSelectorSuppliers<>(
                j.makeKeySelectorSupplier(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, JsonNode> JsonValue(ConnectorConfig config) {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        return new WrapperKeyValueSelectorSuppliers<>(
                OthersSelectorSuppliers.StringKey(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, JsonNode> JsonValue() {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers();
        return new WrapperKeyValueSelectorSuppliers<>(
                OthersSelectorSuppliers.StringKey(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<Object, Object> Object() {
        return new ConnectSelectorsSuppliers();
    }
}
