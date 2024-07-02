
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
import com.lightstreamer.kafka.utils.Either;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GenericRecordSelectorsSuppliers {

    static class GenericRecordBaseSelector extends BaseSelector {

        private static class FieldGetter implements NodeEvaluator<AvroNode> {

            public static AvroNode get(String name, AvroNode node) {
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
            public AvroNode eval(AvroNode node) {
                return get(name, node);
            }
        }

        private static class ArrayGetter implements NodeEvaluator<AvroNode> {

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
            public AvroNode eval(AvroNode node) {
                AvroNode value = getter.eval(node);
                for (GeneralizedKey i : indexes) {
                    if (i.isIndex()) {
                        value = get(i.index(), value);
                    } else {
                        value = FieldGetter.get(i.key(), value);
                    }
                }
                return value;
            }

            private AvroNode get(int index, AvroNode node) {
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
            }
        }

        private static final SelectorExpressionParser<AvroNode> PARSER =
                new SelectorExpressionParser.Builder<AvroNode>()
                        .withFieldEvaluator(FieldGetter::new)
                        .withGenericIndexedEvaluator(ArrayGetter::new)
                        .build();

        private final LinkedNode<NodeEvaluator<AvroNode>> rootNode;

        public GenericRecordBaseSelector(String name, String expression, String expectedRoot) {
            super(name, expression);
            this.rootNode = PARSER.parse(name, expression, expectedRoot);
        }

        protected final Value eval(GenericRecord record) {
            AvroNode node = AvroNode.from(record);
            LinkedNode<NodeEvaluator<AvroNode>> currentNode = rootNode;
            while (currentNode != null) {
                NodeEvaluator<AvroNode> nodeEvaluator = currentNode.value();
                node = nodeEvaluator.eval(node);
                currentNode = currentNode.next();
            }

            if (!node.isScalar()) {
                ValueException.throwNonComplexObjectRequired(expression());
            }

            String text = !node.isNull() ? node.asText() : null;
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

    public static KeySelectorSupplier<GenericRecord> keySelectorSupplier(ConnectorConfig config) {
        return new GenericRecordKeySelectorSupplier(config);
    }

    public static ValueSelectorSupplier<GenericRecord> valueSelectorSupplier(
            ConnectorConfig config) {
        return new GenericRecordValueSelectorSupplier(config);
    }
}

class AvroNode {

    private static final Object NULL_DATA = new Object();

    static AvroNode of(Object avroNode) {
        if (avroNode instanceof GenericContainer container) {
            Schema schema = container.getSchema();
            Type valueType = schema.getType();
            return switch (valueType) {
                case RECORD, FIXED, ARRAY, ENUM -> from(container);
                default -> from(avroNode);
            };
        }
        return from(avroNode);
    }

    static AvroNode from(GenericContainer container) {
        return new AvroNode(container);
    }

    static AvroNode from(Object object) {
        if (object == null) {
            object = NULL_DATA;
        }
        return new AvroNode(object);
    }

    private final Either<GenericContainer, Object> data;

    private AvroNode(GenericContainer container) {
        data = Either.left(container);
    }

    private AvroNode(Object object) {
        data = Either.right(object);
    }

    GenericContainer container() {
        return data.getLeft();
    }

    boolean isContainer() {
        return data.isLeft();
    }

    Object object() {
        return data.getRight();
    }

    boolean isObject() {
        return data.isRight();
    }

    boolean isNull() {
        return isObject() && object() == NULL_DATA;
    }

    boolean has(String name) {
        if (isContainer()) {
            GenericContainer genericContainer = container();
            Schema schema = genericContainer.getSchema();
            if (schema.getType().equals(Type.RECORD)) {
                GenericData.Record record = (GenericData.Record) genericContainer;
                return record.hasField(name);
            }
        }
        if (object() instanceof Map map) {
            return map.containsKey(new Utf8(name));
        }
        return false;
    }

    boolean isArray() {
        if (isContainer()) {
            Schema schema = container().getSchema();
            return schema.getType().equals(Type.ARRAY);
        }
        return false;
    }

    int size() {
        if (isArray()) {
            GenericData.Array<?> array = (GenericData.Array<?>) container();
            return array.size();
        }
        return 0;
    }

    AvroNode get(String name) {
        if (isContainer()) {
            GenericData.Record record = (GenericData.Record) container();
            return AvroNode.of(record.get(name));
        }
        Map<?, ?> map = (Map<?, ?>) object();
        return AvroNode.of(map.get(new Utf8(name)));
    }

    AvroNode get(int index) {
        GenericData.Array<?> array = (GenericData.Array<?>) container();
        return AvroNode.of(array.get(index));
    }

    boolean isScalar() {
        if (isContainer()) {
            Schema schema = container().getSchema();
            Type type = schema.getType();
            return switch (type) {
                case RECORD, ARRAY -> false;
                default -> true;
            };
        }
        return !(object() instanceof Map);
    }

    String asText() {
        if (isContainer()) {
            return container().toString();
        }

        return object().toString();
    }
}
