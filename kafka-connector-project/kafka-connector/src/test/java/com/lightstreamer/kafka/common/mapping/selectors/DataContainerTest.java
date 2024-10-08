
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

package com.lightstreamer.kafka.common.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.common.expressions.Expressions;
import com.lightstreamer.kafka.test_utils.TestSelectorSuppliers;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class DataContainerTest {

    @Test
    public void shouldCreateValue() {
        Data data = Data.from("name", "value");

        assertThat(data.text()).isEqualTo("value");
        assertThat(data.name()).isEqualTo("name");
    }

    @Test
    public void shouldCreateValuesContainer() throws ExtractionException {
        DataExtractor<String, String> extractor =
                DataExtractor.<String, String>builder()
                        .withSuppliers(TestSelectorSuppliers.string())
                        .withSchemaName("schema")
                        .withExpressions(Map.of("name", Expressions.expression("VALUE")))
                        .build();
        DataContainer container =
                DataContainer.from(extractor, Set.of(Data.from("name", "aValue")));

        assertThat(container.extractor()).isSameInstanceAs(extractor);
        assertThat(container.data()).containsExactly(Data.from("name", "aValue"));
    }
}
