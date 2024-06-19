
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

package com.lightstreamer.kafka.adapters.mapping.selectors.avro;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.GeneralizedKey;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka.mapping.selectors.Value;
import com.lightstreamer.kafka.mapping.selectors.ValueException;
import com.lightstreamer.kafka.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.mapping.selectors.ValueSelectorSupplier;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GenericRecordSelectorsSuppliers {

    public static KeySelectorSupplier<GenericRecord> keySelectorSupplier(ConnectorConfig config) {
        return new GenericRecordKeySelectorSupplier(config);
    }

    public static ValueSelectorSupplier<GenericRecord> valueSelectorSupplier(
            ConnectorConfig config) {
        return new GenericRecordValueSelectorSupplier(config);
    }

    private static Logger log = LoggerFactory.getLogger("AvroSupplier");

    static class GenericRecordBaseSelector extends BaseSelector {

        private static class FieldGetter implements NodeEvaluator<GenericRecord, Object> {

            private final String name;

            private FieldGetter(String name) {
                this.name = name;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public Object get(GenericRecord record) {
                return get(name, record);
            }

            public static Object get(String name, GenericRecord record) {
                if (!record.hasField(name)) {
                    ValueException.throwFieldNotFound(name);
                }
                return record.get(name);
            }
        }

        private static class ArrayGetter implements NodeEvaluator<GenericRecord, Object> {

            private final String name;

            private final FieldGetter getter;

            private final List<GeneralizedKey> indexes;

            ArrayGetter(String fieldName, List<GeneralizedKey> indexes) {
                this.name = Objects.requireNonNull(fieldName);
                this.indexes = Objects.requireNonNull(indexes);
                this.getter = new FieldGetter(name);
            }

            @Override
            public String name() {
                return name;
            }

            static Object get(int index, Object value) {
                if (value instanceof GenericData.Array<?> array) {
                    try {
                        if (index < array.size()) {
                            value = array.get(index);
                        } else {
                            throw new IndexOutOfBoundsException();
                        }
                        return value;
                    } catch (IndexOutOfBoundsException ie) {
                        ValueException.throwIndexOfOutBoundex(index);
                    }
                } else {
                    ValueException.throwNoIndexedField();
                }
                // Actually unreachable code
                return null;
            }

            @Override
            public Object get(GenericRecord record) {
                log.atDebug().log("Evaluating record {}", record);
                Object value = getter.get(record);
                log.atDebug().log("Extracted value: {} of type {}", value, value.getClass());
                for (GeneralizedKey i : indexes) {
                    log.atDebug().log("Handling key {}", i);
                    if (i.isIndex()) {
                        value = get(i.index(), value);
                    } else {
                        if (value instanceof Map map) {
                            value = map.get(i.key());
                        } else if (value instanceof GenericRecord gr) {
                            value = FieldGetter.get(i.key().toString(), gr);
                        }
                        if (value == null) {
                            ValueException.throwNoKeyFound(i.key().toString());
                        }
                    }
                }
                return value;
            }
        }

        private LinkedNode<NodeEvaluator<GenericRecord, Object>> linkedNode;

        private static final SelectorExpressionParser<GenericRecord, Object> PARSER =
                new SelectorExpressionParser.Builder<GenericRecord, Object>()
                        .withFieldEvaluator(FieldGetter::new)
                        .withGenericIndexedEvaluator(ArrayGetter::new)
                        .build();

        public GenericRecordBaseSelector(String name, String expression, String expectedRoot) {
            super(name, expression);
            this.linkedNode = PARSER.parse(name, expression, expectedRoot);
        }

        private boolean isScalar(Object value) {
            return !(value instanceof GenericData.Record
                    || value instanceof GenericData.Array
                    || value instanceof Map);
        }

        protected Value eval(GenericRecord record) {
            Object value = record;
            GenericRecord currentRecord = record;
            LinkedNode<NodeEvaluator<GenericRecord, Object>> currentNode = linkedNode;
            while (currentNode != null) {
                if (value == null) {
                    ValueException.throwFieldNotFound(currentNode.value().name());
                    continue;
                }
                if (value instanceof GenericRecord genericRecord) {
                    currentRecord = genericRecord;
                    value = currentNode.value().get(currentRecord);
                    currentNode = currentNode.next();
                    continue;
                }
                ValueException.throwConversionError(value.getClass().getSimpleName());
            }

            if (!isScalar(value)) {
                ValueException.throwNonComplexObjectRequired(expression());
            }

            String text = Objects.toString(value, null);
            return Value.of(name(), text);
        }
    }

    static class GenericRecordKeySelectorSupplier implements KeySelectorSupplier<GenericRecord> {

        private final GenericRecordDeserializer deserializer;

        public GenericRecordKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = new GenericRecordDeserializer(config, true);
        }

        @Override
        public KeySelector<GenericRecord> newSelector(String name, String expression) {
            return new GenericRecordKeySelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
            return deserializer;
        }
    }

    static final class GenericRecordKeySelector extends GenericRecordBaseSelector
            implements KeySelector<GenericRecord> {

        GenericRecordKeySelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<GenericRecord, ?> record) {
            return super.eval(record.key());
        }
    }

    static class GenericRecordValueSelectorSupplier
            implements ValueSelectorSupplier<GenericRecord> {

        private final GenericRecordDeserializer deseralizer;

        GenericRecordValueSelectorSupplier(ConnectorConfig config) {
            this.deseralizer = new GenericRecordDeserializer(config, false);
        }

        @Override
        public ValueSelector<GenericRecord> newSelector(String name, String expression) {
            return new GenericRecordValueSelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
            return deseralizer;
        }
    }

    static final class GenericRecordValueSelector extends GenericRecordBaseSelector
            implements ValueSelector<GenericRecord> {

        public GenericRecordValueSelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<?, GenericRecord> record) {
            return super.eval(record.value());
        }
    }
}
