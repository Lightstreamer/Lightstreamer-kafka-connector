
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

package com.lightstreamer.kafka_connector.adapters.mapping.selectors.others;

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;

import java.util.EnumMap;
import java.util.Objects;

public class OthersSelectorSuppliers {

    private static EnumMap<EvaluatorType, Deserializer<?>> DESERIALIAZERS =
            new EnumMap<>(EvaluatorType.class);

    static {
        DESERIALIAZERS.put(EvaluatorType.INTEGER, new IntegerDeserializer());
        DESERIALIAZERS.put(EvaluatorType.BOOLEAN, new BooleanDeserializer());
        DESERIALIAZERS.put(EvaluatorType.BYTE_ARRAY, new ByteArrayDeserializer());
        DESERIALIAZERS.put(EvaluatorType.BYTE_BUFFER, new ByteBufferDeserializer());
        DESERIALIAZERS.put(EvaluatorType.BYTES, new BytesDeserializer());
        DESERIALIAZERS.put(EvaluatorType.DOUBLE, new DoubleDeserializer());
        DESERIALIAZERS.put(EvaluatorType.FLOAT, new FloatDeserializer());
        DESERIALIAZERS.put(EvaluatorType.LONG, new LongDeserializer());
        DESERIALIAZERS.put(EvaluatorType.SHORT, new ShortDeserializer());
        DESERIALIAZERS.put(EvaluatorType.UUID, new UUIDDeserializer());
    }

    @SuppressWarnings("unchecked")
    public static <T> ValueSelectorSupplier<T> valueSelectorSupplier(EvaluatorType type) {
        return (ValueSelectorSupplier<T>) new OthersValueSelectorSupplier(type);
    }

    @SuppressWarnings("unchecked")
    public static <T> KeySelectorSupplier<T> keySelectorSupplier(EvaluatorType type) {
        return (KeySelectorSupplier<T>) new OthersKeySelectorSupplier(type);
    }

    private static class OthersKeySelectorSupplier implements KeySelectorSupplier<Object> {

        private Deserializer<?> deseralizer;

        OthersKeySelectorSupplier(EvaluatorType type) {
            this.deseralizer = DESERIALIAZERS.get(type);
        }

        @Override
        public boolean maySupply(String expression) {
            return expectedRoot().equals(expression);
        }

        @Override
        public KeySelector<Object> newSelector(String name, String expression) {
            if (!maySupply(expression)) {
                ExpressionException.throwExpectedRootToken(name, expectedRoot());
            }
            return new OthersKeySelector(name, expression);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Deserializer<Object> deseralizer() {
            return (Deserializer<Object>) deseralizer;
        }
    }

    private static final class OthersKeySelector extends BaseSelector
            implements KeySelector<Object> {

        private OthersKeySelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<Object, ?> record) {
            String text = Objects.toString(record.key(), null);
            return Value.of(name(), text);
        }
    }

    private static class OthersValueSelectorSupplier implements ValueSelectorSupplier<Object> {

        private Deserializer<?> deseralizer;

        OthersValueSelectorSupplier(EvaluatorType type) {
            this.deseralizer = DESERIALIAZERS.get(type);
        }

        @Override
        public boolean maySupply(String expression) {
            return expression.equals(expectedRoot());
        }

        @Override
        public ValueSelector<Object> newSelector(String name, String expression) {
            if (!maySupply(expression)) {
                ExpressionException.throwExpectedRootToken(name, expectedRoot());
            }
            return new OthersValueSelector(name, expression);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Deserializer<Object> deseralizer() {
            return (Deserializer<Object>) deseralizer;
        }
    }

    private static class OthersValueSelector extends BaseSelector implements ValueSelector<Object> {

        private OthersValueSelector(String name, String expression) {
            super(name, expression);
        }

        @Override
        public Value extract(ConsumerRecord<?, Object> record) {
            String text = Objects.toString(record.value(), null);
            return Value.of(name(), text);
        }
    }
}
