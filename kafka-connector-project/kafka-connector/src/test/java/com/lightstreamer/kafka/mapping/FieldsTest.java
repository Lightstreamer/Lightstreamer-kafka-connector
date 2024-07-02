
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

package com.lightstreamer.kafka.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.mapping.selectors.Schema;
import com.lightstreamer.kafka.mapping.selectors.SelectorSuppliers;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class FieldsTest {

    @Test
    void shoudCreateBuilderFromMappings() {
        SelectorSuppliers<String, String> selectorSuppliers = TestSelectorSuppliers.string();
        Map<String, String> fieldMappings = Map.of("field1", "#{VALUE}");
        ValuesExtractor<String, String> extractor =
                Fields.fromMapping(fieldMappings, selectorSuppliers);
        Schema schema = extractor.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).isEqualTo(fieldMappings.keySet());
    }
}
