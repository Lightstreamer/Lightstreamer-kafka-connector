
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
import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
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

import java.util.Map;

public class GenericRecordSelectorsSuppliers
        implements KeyValueSelectorSuppliersMaker<GenericRecord> {

    private static class AvroNode implements Node<AvroNode> {

        private static final Object NULL_DATA = new Object();

        static AvroNode fromContainer(GenericContainer container) {
            return new AvroNode(container);
        }

        static AvroNode fromObject(Object object) {
            if (object == null) {
                object = NULL_DATA;
            }
            return new AvroNode(object);
        }

        private static AvroNode of(Object avroNode) {
            if (avroNode instanceof GenericContainer container) {
                Schema schema = container.getSchema();
                Type valueType = schema.getType();
                return switch (valueType) {
                    case RECORD, FIXED, ARRAY, ENUM -> fromContainer(container);
                    default -> fromObject(avroNode);
                };
            }
            return fromObject(avroNode);
        }

        private final Either<GenericContainer, Object> data;

        private AvroNode(GenericContainer container) {
            data = Either.left(container);
        }

        private AvroNode(Object object) {
            data = Either.right(object);
        }

        @Override
        public boolean has(String name) {
            if (isContainer()) {
                GenericContainer genericContainer = container();
                Schema schema = genericContainer.getSchema();
                Type type = schema.getType();
                return switch (type) {
                    case RECORD -> {
                        GenericData.Record record = (GenericData.Record) genericContainer;
                        yield record.hasField(name);
                    }
                    default -> false;
                };
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
                return AvroNode.of(record.get(name));
            }
            Map<?, ?> map = (Map<?, ?>) object();
            return AvroNode.of(map.get(new Utf8(name)));
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
            return AvroNode.of(array.get(index));
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
        public String asText(String defaultStr) {
            if (isNull()) {
                return defaultStr;
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
            this.deserializer = GenericRecordDeserializer.KeyDeserializer(config);
        }

        @Override
        public KeySelector<GenericRecord> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new GenericRecordKeySelector(name, expression);
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
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
        public Data extractKey(KafkaRecord<GenericRecord, ?> record) throws ValueException {
            AvroNode node = AvroNode.fromContainer(record.key());
            return super.eval(node);
        }
    }

    private static class GenericRecordValueSelectorSupplier
            implements ValueSelectorSupplier<GenericRecord> {

        private final Deserializer<GenericRecord> deseralizer;

        GenericRecordValueSelectorSupplier(ConnectorConfig config) {
            this.deseralizer = GenericRecordDeserializer.KeyDeserializer(config);
        }

        @Override
        public ValueSelector<GenericRecord> newSelector(
                String name, ExtractionExpression expression) throws ExtractionException {
            return new GenericRecordValueSelector(name, expression);
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
            return deseralizer;
        }
    }

    private static final class GenericRecordValueSelector extends StructuredBaseSelector<AvroNode>
            implements ValueSelector<GenericRecord> {

        GenericRecordValueSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.VALUE);
        }

        @Override
        public Data extractValue(KafkaRecord<?, GenericRecord> record) throws ValueException {
            AvroNode node = AvroNode.fromContainer(record.value());
            return super.eval(node);
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
