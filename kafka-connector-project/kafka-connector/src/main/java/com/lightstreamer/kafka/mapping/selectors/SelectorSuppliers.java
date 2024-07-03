
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

package com.lightstreamer.kafka.mapping.selectors;

public interface SelectorSuppliers<K, V> {

    KeySelectorSupplier<K> keySelectorSupplier();

    ValueSelectorSupplier<V> valueSelectorSupplier();

    static <K, V> SelectorSuppliers<K, V> of(KeySelectorSupplier<K> k, ValueSelectorSupplier<V> v) {
        return new SelectorSuppliers<K, V>() {

            @Override
            public KeySelectorSupplier<K> keySelectorSupplier() {
                return k;
            }

            @Override
            public ValueSelectorSupplier<V> valueSelectorSupplier() {
                return v;
            }
        };
    }
}
