
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ConstantSelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
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

public class OthersSelectorSuppliers {

    private static EnumMap<EvaluatorType, Deserializer<?>> DESERIALIAZERS =
            new EnumMap<>(EvaluatorType.class);

    static {
        DESERIALIAZERS.put(EvaluatorType.STRING, new StringDeserializer());
        DESERIALIAZERS.put(EvaluatorType.INTEGER, new IntegerDeserializer());
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

    public static ValueSelectorSupplier<?> valueSelectorSupplier(EvaluatorType type) {
        return new OthersValueSelectorSupplier(type);
    }

    public static KeySelectorSupplier<?> keySelectorSupplier(EvaluatorType type) {
        return new OthersKeySelectorSupplier(type);
    }

    private static class BaseOthersSelectorSupplier {
        private final Deserializer<?> deseralizer;
        private final ConstantSelectorSupplier constantSelectorSupplier;

        BaseOthersSelectorSupplier(EvaluatorType type, Constant constant) {
            this.deseralizer = DESERIALIAZERS.get(type);
            this.constantSelectorSupplier = new ConstantSelectorSupplier(constant);
        }

        public KeySelector<Object> newKeySelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return constantSelectorSupplier.newKeySelector(name, expression);
        }

        public ValueSelector<Object> newValueSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return constantSelectorSupplier.newValueSelector(name, expression);
        }

        @SuppressWarnings("unchecked")
        public Deserializer<Object> deseralizer() {
            return (Deserializer<Object>) deseralizer;
        }
    }

    private static class OthersKeySelectorSupplier extends BaseOthersSelectorSupplier
            implements KeySelectorSupplier<Object> {

        OthersKeySelectorSupplier(EvaluatorType type) {
            super(type, Constant.KEY);
        }

        @Override
        public KeySelector<Object> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return newKeySelector(name, expression);
        }
    }

    private static class OthersValueSelectorSupplier extends BaseOthersSelectorSupplier
            implements ValueSelectorSupplier<Object> {

        OthersValueSelectorSupplier(EvaluatorType type) {
            super(type, Constant.VALUE);
        }

        @Override
        public ValueSelector<Object> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return newValueSelector(name, expression);
        }
    }
}
