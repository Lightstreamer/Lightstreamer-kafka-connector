
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

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BOOLEAN;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BYTES;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BYTE_ARRAY;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BYTE_BUFFER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.DOUBLE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.FLOAT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.INTEGER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.SHORT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.STRING;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.UUID;
import static com.lightstreamer.kafka.common.mapping.selectors.ConstantSelectorSupplier.makeSelectorSupplier;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.ConstantSelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.GenericSelector;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorEvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
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
import java.util.Map;

public class OthersSelectorSuppliers implements KeyValueSelectorSuppliersMaker<Object> {

    private static class BaseOthersSelectorSupplier<T> {

        private static EnumMap<EvaluatorType, Deserializer<?>> DESERIALIZERS =
                new EnumMap<>(EvaluatorType.class);

        static {
            DESERIALIZERS.put(STRING, new StringDeserializer());
            DESERIALIZERS.put(INTEGER, new IntegerDeserializer());
            DESERIALIZERS.put(BOOLEAN, new BooleanDeserializer());
            DESERIALIZERS.put(BYTE_ARRAY, new ByteArrayDeserializer());
            DESERIALIZERS.put(BYTE_BUFFER, new ByteBufferDeserializer());
            DESERIALIZERS.put(BYTES, new BytesDeserializer());
            DESERIALIZERS.put(DOUBLE, new DoubleDeserializer());
            DESERIALIZERS.put(FLOAT, new FloatDeserializer());
            DESERIALIZERS.put(LONG, new LongDeserializer());
            DESERIALIZERS.put(SHORT, new ShortDeserializer());
            DESERIALIZERS.put(UUID, new UUIDDeserializer());
        }

        private final Deserializer<T> deserializer;
        protected final EvaluatorType type;

        @SuppressWarnings("unchecked")
        BaseOthersSelectorSupplier(EvaluatorType type, Constant constant) {
            this.type = type;
            this.deserializer = (Deserializer<T>) DESERIALIZERS.get(type);
            if (this.deserializer == null) {
                throw new IllegalArgumentException("Unsupported evaluator [" + type + "]");
            }
        }

        public Deserializer<T> deserializer() {
            return deserializer;
        }

        public SelectorEvaluatorType evaluatorType() {
            return type;
        }
    }

    private static class KeySelectorWrapper<K> implements KeySelector<K> {

        private final GenericSelector selector;

        KeySelectorWrapper(GenericSelector selector) throws ExtractionException {
            this.selector = selector;
        }

        @Override
        public ExtractionExpression expression() {
            return selector.expression();
        }

        @Override
        public Data extractKey(KafkaRecord<K, ?> record, boolean checkScalar)
                throws ValueException {
            return selector.extract(record, checkScalar);
        }

        @Override
        public Data extractKey(java.lang.String name, KafkaRecord<K, ?> record, boolean checkScalar)
                throws ValueException {
            return selector.extract(name, record, checkScalar);
        }

        @Override
        public void extractKeyInto(
                KafkaRecord<K, ?> record, Map<java.lang.String, java.lang.String> target)
                throws ValueException {}
    }

    private static class OthersKeySelectorSupplier<T> extends BaseOthersSelectorSupplier<T>
            implements KeySelectorSupplier<T> {

        private final ConstantSelectorSupplier supplier = makeSelectorSupplier(KEY);

        OthersKeySelectorSupplier(EvaluatorType type) {
            super(type, KEY);
        }

        @Override
        public KeySelector<T> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new KeySelectorWrapper<>(supplier.newSelector(expression));
        }
    }

    private static class ValueSelectorWrapper<V> implements ValueSelector<V> {

        private final GenericSelector selector;

        ValueSelectorWrapper(GenericSelector selector) throws ExtractionException {
            this.selector = selector;
        }

        @Override
        public ExtractionExpression expression() {
            return selector.expression();
        }

        @Override
        public Data extractValue(KafkaRecord<?, V> record, boolean checkScalar)
                throws ValueException {
            return selector.extract(record, checkScalar);
        }

        @Override
        public Data extractValue(
                java.lang.String name, KafkaRecord<?, V> record, boolean checkScalar)
                throws ValueException {
            return selector.extract(name, record, checkScalar);
        }

        @Override
        public void extractValueInto(
                KafkaRecord<?, V> record, Map<java.lang.String, java.lang.String> target)
                throws ValueException {}
    }

    private static class OthersValueSelectorSupplier<T> extends BaseOthersSelectorSupplier<T>
            implements ValueSelectorSupplier<T> {

        private final ConstantSelectorSupplier supplier = makeSelectorSupplier(VALUE);

        OthersValueSelectorSupplier(EvaluatorType type) {
            super(type, VALUE);
        }

        @Override
        public ValueSelector<T> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new ValueSelectorWrapper<T>(supplier.newSelector(expression));
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
        return new OthersKeySelectorSupplier<>(keyEvaluatorType);
    }

    @Override
    public ValueSelectorSupplier<Object> makeValueSelectorSupplier() {
        return new OthersValueSelectorSupplier<>(valueEvaluatorType);
    }

    public static KeySelectorSupplier<String> StringKey() {
        return new OthersKeySelectorSupplier<>(EvaluatorType.fromClass(String.class));
    }

    public static ValueSelectorSupplier<String> StringValue() {
        return new OthersValueSelectorSupplier<>(EvaluatorType.fromClass(String.class));
    }

    public static KeyValueSelectorSuppliers<String, String> String() {
        return KeyValueSelectorSuppliers.of(StringKey(), StringValue());
    }
}
