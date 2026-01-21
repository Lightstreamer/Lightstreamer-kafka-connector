
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

import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_PATH;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs.URL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.AVRO;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.JSON;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.PROTOBUF;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;

import org.apache.avro.generic.GenericRecord;

import java.util.Map;

public interface TestSelectorSuppliers {

    public static KeyValueSelectorSuppliers<GenericRecord, GenericRecord> Avro() {
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(fullAvroConfig());
        return KeyValueSelectorSuppliers.of(
                g.makeKeySelectorSupplier(), g.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, GenericRecord> AvroValue() {
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(fullAvroConfig());
        return KeyValueSelectorSuppliers.of(
                OthersSelectorSuppliers.StringKey(), g.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<GenericRecord, JsonNode> AvroKeyJsonValue() {
        ConnectorConfig config = avroKeyJsonValueConfig();
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        GenericRecordSelectorsSuppliers g = new GenericRecordSelectorsSuppliers(config);
        return KeyValueSelectorSuppliers.of(
                g.makeKeySelectorSupplier(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<JsonNode, JsonNode> Json() {
        ConnectorConfig config = jsonKeyJsonValueConfig();
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        return KeyValueSelectorSuppliers.of(
                j.makeKeySelectorSupplier(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, JsonNode> JsonValue(ConnectorConfig config) {
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        return KeyValueSelectorSuppliers.of(
                OthersSelectorSuppliers.StringKey(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, JsonNode> JsonValue() {
        ConnectorConfig config = avroKeyJsonValueConfig();
        JsonNodeSelectorsSuppliers j = new JsonNodeSelectorsSuppliers(config);
        return KeyValueSelectorSuppliers.of(
                OthersSelectorSuppliers.StringKey(), j.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<String, DynamicMessage> ProtoValue() {
        ConnectorConfig config = protoValueConfig();
        DynamicMessageSelectorSuppliers d = new DynamicMessageSelectorSuppliers(config);
        return KeyValueSelectorSuppliers.of(
                OthersSelectorSuppliers.StringKey(), d.makeValueSelectorSupplier());
    }

    public static KeyValueSelectorSuppliers<Object, Object> Object() {
        ConnectSelectorsSuppliers s = new ConnectSelectorsSuppliers();
        return KeyValueSelectorSuppliers.of(
                s.makeKeySelectorSupplier(), s.makeValueSelectorSupplier());
    }

    private static ConnectorConfig jsonKeyJsonValueConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        JSON.toString(),
                        RECORD_VALUE_EVALUATOR_TYPE,
                        JSON.toString()));
    }    

    private static ConnectorConfig avroKeyJsonValueConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        JSON.toString()));
    }

    private static ConnectorConfig fullAvroConfig() {
        return ConnectorConfigProvider.minimalWith(
                "src/test/resources",
                Map.of(
                        RECORD_KEY_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_KEY_EVALUATOR_SCHEMA_PATH,
                        "value.avsc",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        AVRO.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_PATH,
                        "value.avsc"));
    }

    private static ConnectorConfig protoValueConfig() {
        return ConnectorConfigProvider.minimalWith(
                Map.of(
                        // RECORD_KEY_EVALUATOR_TYPE,
                        // PROTOBUF.toString(),
                        // RECORD_KEY_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        // "true",
                        RECORD_VALUE_EVALUATOR_TYPE,
                        PROTOBUF.toString(),
                        RECORD_VALUE_EVALUATOR_SCHEMA_REGISTRY_ENABLE,
                        "true",
                        URL,
                        "http://localhost:8081"));
    }
}
