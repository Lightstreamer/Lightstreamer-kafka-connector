
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
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.common.utils.Either;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class GenericRecordSelectorsSuppliers
        implements KeyValueSelectorSuppliersMaker<GenericRecord> {

    static class AvroNode implements Node<AvroNode> {

        private static final Object NULL_DATA = new Object();

        static AvroNode from(String name, Object avroNode) {
            if (avroNode instanceof GenericContainer container) {
                Schema schema = container.getSchema();
                Type valueType = schema.getType();
                return switch (valueType) {
                    case RECORD, FIXED, ARRAY, ENUM -> new AvroNode(name, container);
                    // default -> new AvroNode(name, avroNode);
                    default -> throw new RuntimeException("Unsupported Avro type: " + valueType);
                };
            }
            return new AvroNode(name, avroNode);
        }

        private final Either<GenericContainer, Object> data;
        private final String name;

        private AvroNode(String name, GenericContainer container) {
            this.name = name;
            data = Either.left(container);
        }

        private AvroNode(String name, Object object) {
            this.name = name;
            data = Either.right(Objects.requireNonNullElse(object, NULL_DATA));
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            if (isContainer()) {
                GenericContainer genericContainer = container();
                Schema schema = genericContainer.getSchema();
                Type type = schema.getType();
                if (type.equals(Type.RECORD)) {
                    GenericData.Record record = (GenericData.Record) genericContainer;
                    return record.hasField(name);
                }
                return false;
            }

            if (object() instanceof Map map) {
                return map.containsKey(new Utf8(name));
            }
            return false;
        }

        @Override
        public AvroNode get(String name) {
            if (isContainer()) {
                GenericData.Record record = (GenericData.Record) container();
                return AvroNode.from(name, record.get(name));
            }

            Map<?, ?> map = (Map<?, ?>) object();
            return AvroNode.from(name, map.get(new Utf8(name)));
        }

        static Data getAsData(GenericData.Record record, String name) {
            Object obj = record.get(name);
            return Data.from(name, obj != null ? obj.toString() : (String) null);
        }

        @Override
        public boolean isArray() {
            if (isContainer()) {
                Schema schema = container().getSchema();
                return schema.getType().equals(Type.ARRAY);
            }
            return false;
        }

        @Override
        public int size() {
            if (isArray()) {
                GenericData.Array<?> array = (GenericData.Array<?>) container();
                return array.size();
            }
            return 0;
        }

        @Override
        public AvroNode get(int index) {
            GenericData.Array<?> array = (GenericData.Array<?>) container();
            return AvroNode.from(name + "[" + index + "]", array.get(index));
        }

        static Data getAsData(GenericData.Array<?> array, String name, int index) {
            return Data.from(name + "[" + index + "]", Objects.toString(array.get(index), null));
        }

        @Override
        public boolean isNull() {
            if (isContainer()) {
                return false;
            }
            return object() == NULL_DATA;
        }

        @Override
        public boolean isScalar() {
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

        @Override
        public void visit(Consumer<Data> consumer) {
            if (isContainer()) {
                Schema schema = container().getSchema();
                Type type = schema.getType();
                switch (type) {
                    case RECORD -> {
                        GenericData.Record record = (GenericData.Record) container();
                        for (Schema.Field field : record.getSchema().getFields()) {
                            consumer.accept(getAsData(record, field.name()));
                        }
                    }
                    case ARRAY -> {
                        GenericData.Array<?> array = (GenericData.Array<?>) container();
                        for (int i = 0; i < size(); i++) {
                            consumer.accept(getAsData(array, name(), i));
                        }
                    }

                    case FIXED, ENUM -> consumer.accept(Data.from(name, text()));
                    default -> {}
                }
                return;
            }

            if (object() instanceof Map map) {
                for (Object key : map.keySet()) {
                    consumer.accept(
                            Data.from(key.toString(), Objects.toString(map.get(key), null)));
                }
                return;
            }

            consumer.accept(Data.from(name, text()));
        }

        // @Override
        // public List<AvroNode> fields() {
        //     if (isContainer()) {
        //         Schema schema = container().getSchema();
        //         Type type = schema.getType();
        //         return switch (type) {
        //             case RECORD -> {
        //                 GenericData.Record record = (GenericData.Record) container();
        //                 List<AvroNode> list =
        //                         record.getSchema().getFields().stream()
        //                                 .map(field -> get(field.name()))
        //                                 .toList();
        //                 yield list;
        //             }
        //             case ARRAY -> {
        //                 List<AvroNode> array = new ArrayList<>(size());
        //                 for (int i = 0; i < size(); i++) {
        //                     array.add(get(i));
        //                 }
        //                 yield Collections.unmodifiableList(array);
        //             }
        //             default -> Collections.emptyList();
        //         };
        //     }

        //     if (object() instanceof Map map) {
        //         List<AvroNode> fields = new ArrayList<>(map.size());
        //         for (Object key : map.keySet()) {
        //             fields.add(get(key.toString()));
        //         }
        //         return Collections.unmodifiableList(fields);
        //     }
        //     return Collections.emptyList();
        // }

        @Override
        public String text() {
            if (isNull()) {
                return null;
            }
            if (isContainer()) {
                return container().toString();
            }

            return object().toString();
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
    }

    private static class GenericRecordKeySelectorSupplier
            implements KeySelectorSupplier<GenericRecord> {

        private final Deserializer<GenericRecord> deserializer;

        GenericRecordKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = GenericRecordDeserializers.KeyDeserializer(config);
        }

        @Override
        public KeySelector<GenericRecord> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new GenericRecordKeySelector(name, expression);
        }

        @Override
        public Deserializer<GenericRecord> deserializer() {
            return deserializer;
        }
    }

    private static final class GenericRecordKeySelector extends StructuredBaseSelector<AvroNode>
            implements KeySelector<GenericRecord> {

        GenericRecordKeySelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.KEY);
        }

        @Override
        public Data extractKey(KafkaRecord<GenericRecord, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(record::key, AvroNode::new, checkScalar);
        }
    }

    private static class GenericRecordValueSelectorSupplier
            implements ValueSelectorSupplier<GenericRecord> {

        private final Deserializer<GenericRecord> deserializer;

        GenericRecordValueSelectorSupplier(ConnectorConfig config) {
            this.deserializer = GenericRecordDeserializers.ValueDeserializer(config);
        }

        @Override
        public ValueSelector<GenericRecord> newSelector(
                String name, ExtractionExpression expression) throws ExtractionException {
            return new GenericRecordValueSelector(name, expression);
        }

        @Override
        public Deserializer<GenericRecord> deserializer() {
            return deserializer;
        }
    }

    private static final class GenericRecordValueSelector extends StructuredBaseSelector<AvroNode>
            implements ValueSelector<GenericRecord> {

        GenericRecordValueSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.VALUE);
        }

        @Override
        public Data extractValue(KafkaRecord<?, GenericRecord> record, boolean checkScalar)
                throws ValueException {
            return eval(record::value, AvroNode::new, checkScalar);
        }

        @Override
        public Collection<Data> extractValues(KafkaRecord<?, GenericRecord> record)
                throws ValueException {
            return evalMulti(record::value, AvroNode::new);
        }
    }

    private final ConnectorConfig config;

    public GenericRecordSelectorsSuppliers(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public KeySelectorSupplier<GenericRecord> makeKeySelectorSupplier() {
        return new GenericRecordKeySelectorSupplier(config);
    }

    @Override
    public ValueSelectorSupplier<GenericRecord> makeValueSelectorSupplier() {
        return new GenericRecordValueSelectorSupplier(config);
    }
}
