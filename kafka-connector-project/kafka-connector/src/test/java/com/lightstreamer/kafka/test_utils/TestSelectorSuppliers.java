
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

import static com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers.of;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.mapping.selectors.avro.GenericRecordSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.json.JsonNodeSelectorsSuppliers;
import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.connect.mapping.selectors.ConnectSelectorsSuppliers;
import com.lightstreamer.kafka.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValueSelectorSupplier;

import org.apache.avro.generic.GenericRecord;

public interface TestSelectorSuppliers {

    @SuppressWarnings("unchecked")
    public static SelectorSuppliers<String, String> string() {
        return of(
                (KeySelectorSupplier<String>)
                        OthersSelectorSuppliers.keySelectorSupplier(EvaluatorType.STRING),
                (ValueSelectorSupplier<String>)
                        OthersSelectorSuppliers.valueSelectorSupplier(EvaluatorType.STRING));
    }

    public static SelectorSuppliers<GenericRecord, GenericRecord> avro(ConnectorConfig config) {
        return of(
                GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
                GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
    }

    @SuppressWarnings("unchecked")
    public static SelectorSuppliers<String, GenericRecord> avroValue(ConnectorConfig config) {
        return of(
                (KeySelectorSupplier<String>)
                        OthersSelectorSuppliers.keySelectorSupplier(EvaluatorType.STRING),
                GenericRecordSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static SelectorSuppliers<GenericRecord, JsonNode> avroKeyJsonValue(
            ConnectorConfig config) {
        return of(
                GenericRecordSelectorsSuppliers.keySelectorSupplier(config),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static SelectorSuppliers<JsonNode, JsonNode> json(ConnectorConfig config) {
        return of(
                JsonNodeSelectorsSuppliers.keySelectorSupplier(config),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    @SuppressWarnings("unchecked")
    public static SelectorSuppliers<String, JsonNode> jsonValue(ConnectorConfig config) {
        return of(
                (KeySelectorSupplier<String>)
                        OthersSelectorSuppliers.keySelectorSupplier(EvaluatorType.STRING),
                JsonNodeSelectorsSuppliers.valueSelectorSupplier(config));
    }

    public static SelectorSuppliers<Object, Object> object() {
        return of(
                ConnectSelectorsSuppliers.keySelectorSupplier(),
                ConnectSelectorsSuppliers.valueSelectorSupplier());
    }
}
