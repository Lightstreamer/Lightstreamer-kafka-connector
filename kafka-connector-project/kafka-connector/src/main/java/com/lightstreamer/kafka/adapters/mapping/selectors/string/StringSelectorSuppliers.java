
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

package com.lightstreamer.kafka.adapters.mapping.selectors.string;

import com.lightstreamer.kafka.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.Value;
import com.lightstreamer.kafka.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StringSelectorSuppliers {

    private static final StringKeySelectorSupplier KEY_SELCTOR_SUPPLIER =
            new StringKeySelectorSupplier();

    private static final StringValueSelectorSupplier VALUE_SELECTOR_SUPPLIER =
            new StringValueSelectorSupplier();

    public static ValueSelectorSupplier<String> valueSelectorSupplier() {
        return VALUE_SELECTOR_SUPPLIER;
    }

    public static KeySelectorSupplier<String> keySelectorSupplier() {
        return KEY_SELCTOR_SUPPLIER;
    }

    private static class StringKeySelectorSupplier implements KeySelectorSupplier<String> {

        private StringDeserializer deseralizer;

        StringKeySelectorSupplier() {
            this.deseralizer = new StringDeserializer();
        }

        @Override
        public boolean maySupply(String expression) {
            return expectedRoot().equals(expression);
        }

        @Override
        public KeySelector<String> newSelector(String name, String expression) {
            if (!maySupply(expression)) {
                throw ExpressionException.throwExpectedRootToken(name, expectedRoot());
            }
            return new StringKeySelector(name, expression);
        }

        @Override
        public Deserializer<String> deseralizer() {
            return deseralizer;
        }
    }

    private static final class StringKeySelector extends BaseSelector
            implements KeySelector<String> {

        private StringKeySelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(KafkaRecord<String, ?> record) {
            return Value.of(name(), record.key());
        }
    }

    private static class StringValueSelectorSupplier implements ValueSelectorSupplier<String> {

        private StringDeserializer deseralizer;

        StringValueSelectorSupplier() {
            this.deseralizer = new StringDeserializer();
        }

        @Override
        public boolean maySupply(String expression) {
            return expression.equals(expectedRoot());
        }

        @Override
        public ValueSelector<String> newSelector(String name, String expression) {
            if (!maySupply(expression)) {
                throw ExpressionException.throwExpectedRootToken(name, expectedRoot());
            }
            return new StringValueSelector(name, expression);
        }

        @Override
        public Deserializer<String> deseralizer() {
            return deseralizer;
        }
    }

    private static class StringValueSelector extends BaseSelector implements ValueSelector<String> {

        private StringValueSelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(KafkaRecord<?, String> record) {
            return Value.of(name(), record.value());
        }
    }
}
