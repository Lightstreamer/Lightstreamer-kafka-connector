
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

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.adapters.mapping.selectors.WrapperKeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.ConstantSelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
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

        private static EnumMap<EvaluatorType, Deserializer<?>> DESERIALIZERS =
                new EnumMap<>(EvaluatorType.class);

        static {
            DESERIALIZERS.put(EvaluatorType.STRING, new StringDeserializer());
            DESERIALIZERS.put(EvaluatorType.INTEGER, new IntegerDeserializer());
            DESERIALIZERS.put(EvaluatorType.BOOLEAN, new BooleanDeserializer());
            DESERIALIZERS.put(EvaluatorType.BYTE_ARRAY, new ByteArrayDeserializer());
            DESERIALIZERS.put(EvaluatorType.BYTE_BUFFER, new ByteBufferDeserializer());
            DESERIALIZERS.put(EvaluatorType.BYTES, new BytesDeserializer());
            DESERIALIZERS.put(EvaluatorType.DOUBLE, new DoubleDeserializer());
            DESERIALIZERS.put(EvaluatorType.FLOAT, new FloatDeserializer());
            DESERIALIZERS.put(EvaluatorType.LONG, new LongDeserializer());
            DESERIALIZERS.put(EvaluatorType.SHORT, new ShortDeserializer());
            DESERIALIZERS.put(EvaluatorType.UUID, new UUIDDeserializer());
        }

        private final Deserializer<T> deserializer;

        @SuppressWarnings("unchecked")
        BaseOthersSelectorSupplier(EvaluatorType type, Class<T> klass, Constant constant) {
            this.deserializer = (Deserializer<T>) DESERIALIZERS.get(type);
        }

        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }

    private static class OthersKeySelectorSupplier<T> extends BaseOthersSelectorSupplier<T>
            implements KeySelectorSupplier<T> {

        private final ConstantSelectorSupplier constantSelectorSupplier =
                ConstantSelectorSupplier.KeySelector();

        OthersKeySelectorSupplier(EvaluatorType type, Class<T> klass) {
            super(type, klass, KEY);
        }

        @SuppressWarnings("unchecked")
        @Override
        public KeySelector<T> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return (KeySelector<T>) constantSelectorSupplier.newSelector(name, expression);
        }
    }

    private static class OthersValueSelectorSupplier<T> extends BaseOthersSelectorSupplier<T>
            implements ValueSelectorSupplier<T> {

        private final ConstantSelectorSupplier selectorSupplier =
                ConstantSelectorSupplier.ValueSelector();

        OthersValueSelectorSupplier(EvaluatorType type, Class<T> klass) {
            super(type, klass, Constant.VALUE);
        }

        @SuppressWarnings("unchecked")
        @Override
        public ValueSelector<T> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return (ValueSelector<T>) selectorSupplier.newSelector(name, expression);
        }
    }

    private final EvaluatorType keyEvaluatorType;
    private final EvaluatorType valueEvaluatorType;

    public OthersSelectorSuppliers(ConnectorConfig config) {
        this.keyEvaluatorType = config.getKeyEvaluator();
        this.valueEvaluatorType = config.getValueEvaluator();
    }

    public EvaluatorType keyEvaluatorType() {
        return keyEvaluatorType;
    }

    public EvaluatorType valueEvaluatorType() {
        return valueEvaluatorType;
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
        return new WrapperKeyValueSelectorSuppliers<>(StringKey(), StringValue());
    }
}
