
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

package com.lightstreamer.kafka.connect.mapping.selectors;

import com.lightstreamer.kafka.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord.KafkaSinkRecord;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.GeneralizedKey;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka.mapping.selectors.Value;
import com.lightstreamer.kafka.mapping.selectors.ValueException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConnectSelectorsSuppliers {

    static class ConnectBaseSelector extends BaseSelector {

        private static class FieldGetter implements NodeEvaluator<SchemaAndValueNode> {

            public static SchemaAndValueNode get(String name, SchemaAndValueNode node) {
                if (!node.has(name)) {
                    ValueException.throwFieldNotFound(name);
                }
                return node.get(name);
            }

            private final String name;

            private FieldGetter(String name) {
                this.name = name;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public SchemaAndValueNode eval(SchemaAndValueNode composite) {
                return get(name, composite);
            }
        }

        private static class ArrayGetter implements NodeEvaluator<SchemaAndValueNode> {

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

            @Override
            public SchemaAndValueNode eval(SchemaAndValueNode node) {
                SchemaAndValueNode value = getter.eval(node);
                for (GeneralizedKey i : indexes) {
                    if (i.isIndex()) {
                        value = get(i.index(), value);
                    } else {
                        value = FieldGetter.get(i.key(), value);
                    }
                }
                return value;
            }

            SchemaAndValueNode get(int index, SchemaAndValueNode node) {
                if (node.isArray()) {
                    if (index < node.size()) {
                        return node.get(index);
                    } else {
                        ValueException.throwIndexOfOutBoundex(index);
                        // Actually unreachable code
                        return null;
                    }
                } else {
                    ValueException.throwNoIndexedField(name);
                    // Actually unreachable code
                    return null;
                }
                // if (node.schema().type().equals(Schema.Type.ARRAY)) {
                //     Schema elementsSchema = node.schema().valueSchema();
                //     @SuppressWarnings("unchecked")
                //     List<Object> array = (List<Object>) node.value();
                //     if (index < array.size()) {
                //         return new SchemaAndValue(elementsSchema, array.get(index));
                //     } else {
                //         ValueException.throwIndexOfOutBoundex(index);
                //         // Actually unreachable code
                //         return null;
                //     }
                // } else {
                //     ValueException.throwNoIndexedField(name);
                //     // Actually unreachable code
                //     return null;
                // }
            }
        }

        private static final SelectorExpressionParser<SchemaAndValueNode> PARSER =
                new SelectorExpressionParser.Builder<SchemaAndValueNode>()
                        .withFieldEvaluator(FieldGetter::new)
                        .withGenericIndexedEvaluator(ArrayGetter::new)
                        .build();

        private LinkedNode<NodeEvaluator<SchemaAndValueNode>> rootNode;

        public ConnectBaseSelector(String name, String expression, String expectedRoot) {
            super(name, expression);
            this.rootNode = PARSER.parse(name, expression, expectedRoot);
        }

        protected final Value eval(Object object, Schema schema) {
            SchemaAndValueNode node = new SchemaAndValueNode(new SchemaAndValue(schema, object));
            LinkedNode<NodeEvaluator<SchemaAndValueNode>> currentLinkedNode = rootNode;
            while (currentLinkedNode != null) {
                NodeEvaluator<SchemaAndValueNode> nodeEvaluator = currentLinkedNode.value();
                node = nodeEvaluator.eval(node);
                currentLinkedNode = currentLinkedNode.next();
            }

            if (!node.isScalar()) {
                ValueException.throwNonComplexObjectRequired(expression());
            }

            return Value.of(name(), node.asText());
        }
    }

    static class ConnectKeySelectorSupplierImpl implements ConnectKeySelectorSupplier {

        public ConnectKeySelectorSupplierImpl() {}

        @Override
        public ConnectKeySelector newSelector(String name, String expression) {
            return new ConnectKeySelectorImpl(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<Object> deseralizer() {
            throw new UnsupportedOperationException();
        }

        public boolean maySupply(String expression) {
            return expression.equals(expectedRoot())
                    || ConnectKeySelectorSupplier.super.maySupply(expression);
        }
    }

    static class ConnectKeySelectorImpl extends ConnectBaseSelector implements ConnectKeySelector {

        public ConnectKeySelectorImpl(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<Object, ?> record) {
            KafkaRecord.KafkaSinkRecord sinkRecord = (KafkaSinkRecord) record;
            return super.eval(record.key(), sinkRecord.keySchema());
        }
    }

    static class ConnectValueSelectorSupplierImpl implements ConnectValueSelectorSupplier {

        public ConnectValueSelectorSupplierImpl() {}

        @Override
        public ConnectValueSelector newSelector(String name, String expression) {
            return new ConnectValueSelectorImpl(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<Object> deseralizer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean maySupply(String expression) {
            return expression.equals(expectedRoot())
                    || ConnectValueSelectorSupplier.super.maySupply(expression);
        }
    }

    static class ConnectValueSelectorImpl extends ConnectBaseSelector
            implements ConnectValueSelector {

        public ConnectValueSelectorImpl(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<?, Object> record) {
            KafkaRecord.KafkaSinkRecord sinkRecord = (KafkaSinkRecord) record;
            return super.eval(record.value(), sinkRecord.valueSchema());
        }
    }

    public static ConnectKeySelectorSupplier keySelectorSupplier() {
        return new ConnectKeySelectorSupplierImpl();
    }

    public static ConnectValueSelectorSupplier valueSelectorSupplier() {
        return new ConnectValueSelectorSupplierImpl();
    }
}

class SchemaAndValueNode {

    private final SchemaAndValue data;

    SchemaAndValueNode(SchemaAndValue data) {
        this.data = data;
    }

    boolean has(String name) {
        Schema schema = data.schema();
        return switch (schema.type()) {
            case STRUCT -> schema.field(name) != null;

            case MAP -> {
                Map<String, ?> map = (Map<String, ?>) data.value();
                yield map.containsKey(name);
            }

            default -> false;
        };
    }

    boolean isArray() {
        return data.schema().type().equals(Schema.Type.ARRAY);
    }

    int size() {
        if (isArray()) {
            List<Object> array = (List<Object>) data.value();
            return array.size();
        }
        return 0;
    }

    SchemaAndValueNode get(String name) {
        Schema schema = data.schema();

        SchemaAndValue schemaAndValue =
                switch (schema.type()) {
                    case MAP -> {
                        Schema keySchema = schema.keySchema();
                        Schema valueSchema = schema.valueSchema();
                        Map<String, ?> map = (Map<String, ?>) data.value();
                        yield new SchemaAndValue(valueSchema, map.get(name));
                    }

                    default -> {
                        Struct struct = (Struct) data.value();
                        Field field = schema.field(name);
                        yield new SchemaAndValue(field.schema(), struct.get(field));
                    }
                };
        return new SchemaAndValueNode(schemaAndValue);
    }

    SchemaAndValueNode get(int index) {
        List<Object> array = (List<Object>) data.value();
        Schema elementsSchema = data.schema().valueSchema();
        return new SchemaAndValueNode(new SchemaAndValue(elementsSchema, array.get(index)));
    }

    boolean isScalar() {
        return data.schema().type().isPrimitive();
    }

    String asText() {
        String text = null;
        Object value = data.value();
        if (value != null) {
            if (value instanceof ByteBuffer buffer) {
                text = Arrays.toString(buffer.array());
            } else if (value instanceof byte[] bt) {
                text = Arrays.toString(bt);
            } else {
                text = value.toString();
            }
        }
        return text;
    }
}
