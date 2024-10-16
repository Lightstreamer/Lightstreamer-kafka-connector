
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

package com.lightstreamer.kafka.adapters.mapping.selectors.others;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.mapping.selectors.AdapterKeyValueSelectorSupplier;
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ConstantSelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;

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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;

import java.util.EnumMap;

public class OthersSelectorSuppliers implements KeyValueSelectorSuppliersMaker<Object> {

    private static class BaseOthersSelectorSupplier<T> {
        protected final Deserializer<T> deseralizer;
        private final ConstantSelectorSupplier constantSelectorSupplier;

        @SuppressWarnings("unchecked")
        BaseOthersSelectorSupplier(EvaluatorType type, Class<T> klass, Constant constant) {
            this.deseralizer = (Deserializer<T>) DESERIALIAZERS.get(type);
            this.constantSelectorSupplier = new ConstantSelectorSupplier(constant);
        }

        @SuppressWarnings("unchecked")
        public KeySelector<T> newKeySelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return (KeySelector<T>) constantSelectorSupplier.newKeySelector(name, expression);
        }

        @SuppressWarnings("unchecked")
        public ValueSelector<T> newValueSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return (ValueSelector<T>) constantSelectorSupplier.newValueSelector(name, expression);
        }

        public Deserializer<T> deseralizer() {
            return deseralizer;
        }
    }

    private static class OthersKeySelectorSupplier<T> extends BaseOthersSelectorSupplier<T>
            implements KeySelectorSupplier<T> {

        OthersKeySelectorSupplier(EvaluatorType type, Class<T> klass) {
            super(type, klass, Constant.KEY);
        }

        @Override
        public KeySelector<T> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return newKeySelector(name, expression);
        }
    }

    private static class OthersValueSelectorSupplier<T> extends BaseOthersSelectorSupplier<T>
            implements ValueSelectorSupplier<T> {

        OthersValueSelectorSupplier(EvaluatorType type, Class<T> klass) {
            super(type, klass, Constant.VALUE);
        }

        @Override
        public ValueSelector<T> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return newValueSelector(name, expression);
        }
    }

    private static EnumMap<EvaluatorType, Deserializer<?>> DESERIALIAZERS =
            new EnumMap<>(EvaluatorType.class);

    static {
        DESERIALIAZERS.put(EvaluatorType.STRING, new StringDeserializer());
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

    private final EvaluatorType keyEvaluatorType;
    private final EvaluatorType valueEvaluatorType;

    public OthersSelectorSuppliers(ConnectorConfig config) {
        this(config.getKeyEvaluator(), config.getValueEvaluator());
    }

    public OthersSelectorSuppliers(EvaluatorType keyValueType) {
        this(keyValueType, keyValueType);
    }

    public OthersSelectorSuppliers(EvaluatorType keyType, EvaluatorType valueType) {
        this.keyEvaluatorType = keyType;
        this.valueEvaluatorType = valueType;
    }

    @Override
    public KeySelectorSupplier<Object> makeKeySelectorSupplier() {
        return new OthersKeySelectorSupplier<>(keyEvaluatorType, Object.class);
    }

    @Override
    public ValueSelectorSupplier<Object> makeValueSelectorSupplier() {
        return new OthersValueSelectorSupplier<>(valueEvaluatorType, Object.class);
    }

    public static <K> KeySelectorSupplier<K> KeySelectorSupplier(Class<K> klass) {
        return new OthersKeySelectorSupplier<>(EvaluatorType.fromClass(klass), klass);
    }

    public static <V> ValueSelectorSupplier<V> ValueSelectorSupplier(Class<V> klass) {
        return new OthersValueSelectorSupplier<>(EvaluatorType.fromClass(klass), klass);
    }

    public static KeySelectorSupplier<String> StringKey() {
        return KeySelectorSupplier(String.class);
    }

    public static ValueSelectorSupplier<String> StringValue() {
        return ValueSelectorSupplier(String.class);
    }

    public static KeyValueSelectorSuppliers<String, String> String() {
        return new AdapterKeyValueSelectorSupplier<>(StringKey(), StringValue());
    }
}
