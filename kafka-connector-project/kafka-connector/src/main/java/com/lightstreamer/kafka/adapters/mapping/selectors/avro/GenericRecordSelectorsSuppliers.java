
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
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.mapping.selectors.StructuredBaseSelector;
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

import java.util.Map;

public class GenericRecordSelectorsSuppliers {

    private static class AvroNode implements Node<AvroNode> {

        private static final Object NULL_DATA =
                new Object() {
                    @Override
                    public String toString() {
                        return "NULL_DATA";
                    }
                };

        static AvroNode from(GenericContainer container) {
            return new AvroNode(container);
        }

        private static AvroNode of(Object avroNode) {
            if (avroNode instanceof GenericContainer container) {
                Schema schema = container.getSchema();
                Type valueType = schema.getType();
                return switch (valueType) {
                    case RECORD, FIXED, ARRAY, ENUM -> from(container);
                    default -> new AvroNode(avroNode);
                };
            }
            return new AvroNode(avroNode);
        }

        private final Either<GenericContainer, Object> data;

        private AvroNode(GenericContainer container) {
            data = Either.left(container);
        }

        private AvroNode(Object object) {
            if (object == null) {
                object = NULL_DATA;
            }
            data = Either.right(object);
        }

        @Override
        public boolean has(String name) {
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

        private final GenericRecordDeserializer deserializer;

        GenericRecordKeySelectorSupplier(ConnectorConfig config) {
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

    private static final class GenericRecordKeySelector extends StructuredBaseSelector<AvroNode>
            implements KeySelector<GenericRecord> {
        GenericRecordKeySelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<GenericRecord, ?> record) throws ValueException {
            AvroNode node = AvroNode.from(record.key());
            return super.eval(node);
        }
    }

    private static class GenericRecordValueSelectorSupplier
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

    private static final class GenericRecordValueSelector extends StructuredBaseSelector<AvroNode>
            implements ValueSelector<GenericRecord> {

        GenericRecordValueSelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(KafkaRecord<?, GenericRecord> record) throws ValueException {
            AvroNode node = AvroNode.from(record.value());
            return super.eval(node);
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
