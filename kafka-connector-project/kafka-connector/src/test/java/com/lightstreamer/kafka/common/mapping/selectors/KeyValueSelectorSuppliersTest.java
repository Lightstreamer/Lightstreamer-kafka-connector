
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

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

public class KeyValueSelectorSuppliersTest {

    static class KeySelectorSupplierImpl<T> implements KeySelectorSupplier<T> {

        private Deserializer<T> deserializer;

        KeySelectorSupplierImpl(Deserializer<T> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public KeySelector<T> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            throw new UnsupportedOperationException("Unimplemented method 'newSelector'");
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            throw new UnsupportedOperationException("Unimplemented method 'evaluatorType'");
        }
    }

    static class ValueSelectorSupplierImpl<T> implements ValueSelectorSupplier<T> {

        private Deserializer<T> deserializer;

        ValueSelectorSupplierImpl(Deserializer<T> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public ValueSelector<T> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            throw new UnsupportedOperationException("Unimplemented method 'newSelector'");
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            throw new UnsupportedOperationException("Unimplemented method 'evaluatorType'");
        }
    }

    @Test
    void shouldCreate() {
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        KeySelectorSupplierImpl<String> keySelectorSupplier =
                new KeySelectorSupplierImpl<String>(stringDeserializer);
        Deserializer<Long> longDeserializer = Serdes.Long().deserializer();
        ValueSelectorSupplierImpl<Long> valueSelectorSupplier =
                new ValueSelectorSupplierImpl<Long>(longDeserializer);
        KeyValueSelectorSuppliers<String, Long> adapterKeyValueSelectorSupplier =
                KeyValueSelectorSuppliers.of(keySelectorSupplier, valueSelectorSupplier);

        assertThat(adapterKeyValueSelectorSupplier.keySelectorSupplier())
                .isSameInstanceAs(keySelectorSupplier);
        assertThat(adapterKeyValueSelectorSupplier.valueSelectorSupplier())
                .isSameInstanceAs(valueSelectorSupplier);
        assertThat(adapterKeyValueSelectorSupplier.keySelectorSupplier().deserializer())
                .isSameInstanceAs(stringDeserializer);
        assertThat(adapterKeyValueSelectorSupplier.valueSelectorSupplier().deserializer())
                .isSameInstanceAs(longDeserializer);
    }
}
