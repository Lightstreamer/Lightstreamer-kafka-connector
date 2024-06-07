
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

package com.lightstreamer.kafka.connect.mapping;

import com.lightstreamer.kafka_connector.adapters.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KafkaRecord.KafkaSinkRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.SelectorExpressionParser.GeneralizedKey;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueException;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConnectSelectorsSuppliers {

    public static KeySelectorSupplier<Object> keySelectorSupplier() {
        return new ConnectKeySelectorSupplier();
    }

    public static ValueSelectorSupplier<Object> valueSelectorSupplier() {
        return new ConnectValueSelectorSupplier();
    }

    private static Logger log = LoggerFactory.getLogger("ConnectSelectorsSuppliers");

    static class ConnectBaseSelector extends BaseSelector {

        private static class FieldGetter implements NodeEvaluator<SchemaAndValue, SchemaAndValue> {

            private final String name;

            private FieldGetter(String name) {
                this.name = name;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public SchemaAndValue get(SchemaAndValue composite) {
                return get(name, composite);
            }

            public static SchemaAndValue get(String name, SchemaAndValue composite) {
                Field field = composite.schema().field(name);
                if (field == null) {
                    ValueException.throwFieldNotFound(name);
                }
                Struct struct = (Struct) composite.value();
                Object value = struct.get(field);

                return new SchemaAndValue(field.schema(), value);
            }
        }

        private static class ArrayGetter implements NodeEvaluator<SchemaAndValue, SchemaAndValue> {

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

            static SchemaAndValue get(int index, SchemaAndValue record) {
                if (record.schema().type().equals(Schema.Type.ARRAY)) {
                    Schema elementsSchema = record.schema().valueSchema();
                    @SuppressWarnings("unchecked")
                    List<Object> array = (List<Object>) record.value();
                    if (index < array.size()) {
                        return new SchemaAndValue(elementsSchema, array.get(index));
                    } else {
                        ValueException.throwIndexOfOutBoundex(index);
                        // Actually unreachable code
                        return null;
                    }
                } else {
                    ValueException.throwNoIndexedField();
                    // Actually unreachable code
                    return null;
                }
            }

            @Override
            public SchemaAndValue get(SchemaAndValue record) {
                SchemaAndValue schemAndValue = getter.get(record);
                for (GeneralizedKey i : indexes) {
                    if (i.isIndex()) {
                        schemAndValue = get(i.index(), schemAndValue);
                    } else {
                        Schema schema = schemAndValue.schema();
                        Object value = schemAndValue.value();
                        schemAndValue =
                                switch (schema.type()) {
                                    case MAP -> {
                                        Schema keySchema = schema.keySchema();
                                        Schema valueSchema = schema.valueSchema();
                                        @SuppressWarnings("unchecked")
                                        Map<String, ?> map = (Map<String, ?>) value;
                                        yield new SchemaAndValue(
                                                valueSchema, map.get(i.key().toString()));
                                    }

                                    default -> FieldGetter.get(i.key().toString(), schemAndValue);
                                };
                    }
                }
                return schemAndValue;
            }
        }

        private LinkedNode<NodeEvaluator<SchemaAndValue, SchemaAndValue>> rootNode;

        private static final SelectorExpressionParser<SchemaAndValue, SchemaAndValue> PARSER =
                new SelectorExpressionParser.Builder<SchemaAndValue, SchemaAndValue>()
                        .withFieldEvaluator(FieldGetter::new)
                        .withGenericIndexedEvaluator(ArrayGetter::new)
                        .build();

        public ConnectBaseSelector(String name, String expression, String expectedRoot) {
            super(name, expression);
            this.rootNode = PARSER.parse(name, expression, expectedRoot);
        }

        private boolean isScalar(SchemaAndValue value) {
            return value.schema().type().isPrimitive();
        }

        protected Value eval(Object object, Schema schema) {
            SchemaAndValue container = new SchemaAndValue(schema, object);
            LinkedNode<NodeEvaluator<SchemaAndValue, SchemaAndValue>> currentLinkedNode = rootNode;
            while (currentLinkedNode != null) {
                NodeEvaluator<SchemaAndValue, SchemaAndValue> nodeEvaluator =
                        currentLinkedNode.value();
                container = nodeEvaluator.get(container);
                currentLinkedNode = currentLinkedNode.next();
            }

            if (!isScalar(container)) {
                ValueException.throwNonComplexObjectRequired(expression());
            }

            Object value = container.value();
            String text = null;
            if (value != null) {
                if (value instanceof ByteBuffer buffer) {
                    text = Arrays.toString(buffer.array());
                } else if (value instanceof byte[] bt) {
                    text = Arrays.toString(bt);
                } else {
                    text = value.toString();
                }
            }
            return Value.of(name(), text);
        }
    }

    static class ConnectKeySelectorSupplier implements KeySelectorSupplier<Object> {

        public ConnectKeySelectorSupplier() {}

        @Override
        public KeySelector<Object> newSelector(String name, String expression) {
            return new ConnectKeySelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<Object> deseralizer() {
            throw new UnsupportedOperationException();
        }
    }

    static class ConnectKeySelector extends ConnectBaseSelector implements KeySelector<Object> {

        public ConnectKeySelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<Object, ?> record) {
            KafkaRecord.KafkaSinkRecord sinkRecord = (KafkaSinkRecord) record;
            return super.eval(record.value(), sinkRecord.valueSchema());
        }
    }

    static class ConnectValueSelectorSupplier implements ValueSelectorSupplier<Object> {

        public ConnectValueSelectorSupplier() {}

        @Override
        public ValueSelector<Object> newSelector(String name, String expression) {
            return new ConnectValueSelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<Object> deseralizer() {
            throw new UnsupportedOperationException();
        }
    }

    static class ConnectValueSelector extends ConnectBaseSelector implements ValueSelector<Object> {

        public ConnectValueSelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<?, Object> record) {
            KafkaRecord.KafkaSinkRecord sinkRecord = (KafkaSinkRecord) record;
            return super.eval(record.value(), sinkRecord.valueSchema());
        }
    }
}
