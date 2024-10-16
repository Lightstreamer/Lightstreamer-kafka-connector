
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

/**
 * The interface for wrapping a key selector supplier and a value selector supplier for the given
 * data type.
 */
public interface KeyValueSelectorSuppliers<K, V> {

    SelectorSupplier<KeySelector<K>> keySelectorSupplier();

    SelectorSupplier<ValueSelector<V>> valueSelectorSupplier();

    static <K, V> KeyValueSelectorSuppliers<K, V> from(
            SelectorSupplier<KeySelector<K>> k, SelectorSupplier<ValueSelector<V>> v) {
        return new WrapperSelectors<>(k, v);
    }

    public static class WrapperSelectors<K, V> implements KeyValueSelectorSuppliers<K, V> {

        private final SelectorSupplier<KeySelector<K>> keySelectorSupplier;
        private final SelectorSupplier<ValueSelector<V>> valueSelectorSupplier;

        public WrapperSelectors(
                SelectorSupplier<KeySelector<K>> keySelectorSupplier,
                SelectorSupplier<ValueSelector<V>> valueSelectorSupplier) {
            this.keySelectorSupplier = keySelectorSupplier;
            this.valueSelectorSupplier = valueSelectorSupplier;
        }

        @Override
        public SelectorSupplier<KeySelector<K>> keySelectorSupplier() {
            return keySelectorSupplier;
        }

        @Override
        public SelectorSupplier<ValueSelector<V>> valueSelectorSupplier() {
            return valueSelectorSupplier;
        }
    }
}
