
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

package com.lightstreamer.kafka.adapters.mapping.selectors;

import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.Deserializer;

/** Default implementation of the {@code KeyValueSelectorSuppliers} interface. */
public class WrapperKeyValueSelectorSuppliers<K, V> implements KeyValueSelectorSuppliers<K, V> {

    public interface KeyValueDeserializers<K, V> {

        Deserializer<K> keyDeserializer();

        Deserializer<V> valueDeserializer();
    }

    private static class WrapperDeserializers<K, V> implements KeyValueDeserializers<K, V> {

        private final Deserializer<K> keyDeserializer;
        private final Deserializer<V> valueDeserializer;

        WrapperDeserializers(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
        }

        @Override
        public Deserializer<K> keyDeserializer() {
            return keyDeserializer;
        }

        @Override
        public Deserializer<V> valueDeserializer() {
            return valueDeserializer;
        }
    }

    protected final KeySelectorSupplier<K> keySelectorSupplier;
    protected final ValueSelectorSupplier<V> valueSelectorSupplier;
    private final KeyValueDeserializers<K, V> deserializers;

    public WrapperKeyValueSelectorSuppliers(
            KeySelectorSupplier<K> keySelectorSupplier,
            ValueSelectorSupplier<V> valueSelectorSupplier) {
        this.keySelectorSupplier = keySelectorSupplier;
        this.valueSelectorSupplier = valueSelectorSupplier;
        this.deserializers =
                new WrapperDeserializers<>(
                        keySelectorSupplier.deserializer(), valueSelectorSupplier.deserializer());
    }

    @Override
    public KeySelectorSupplier<K> keySelectorSupplier() {
        return keySelectorSupplier;
    }

    @Override
    public ValueSelectorSupplier<V> valueSelectorSupplier() {
        return valueSelectorSupplier;
    }

    public KeyValueDeserializers<K, V> deserializers() {
        return deserializers;
    }
}
