
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
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorEvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

public class GenericRecordSelectorsSuppliers
        implements KeyValueSelectorSuppliersMaker<GenericRecord> {

    interface AvroNode extends Node<AvroNode> {

        @Override
        default AvroNode get(String nodeName, String propertyName) {
            return InvalidNode.INSTANCE;
        }

        @Override
        default boolean has(String propertyname) {
            return false;
        }

        @Override
        default AvroNode get(String nodeName, int index) {
            return InvalidNode.INSTANCE;
        }

        default boolean isArray() {
            return false;
        }

        default boolean isScalar() {
            return false;
        }

        default int size() {
            return 0;
        }

        static class InvalidNode implements AvroNode {

            static final InvalidNode INSTANCE = new InvalidNode();

            @Override
            public String name() {
                return "INVALID NODE";
            }

            @Override
            public String text() {
                return "INVALID NODE";
            }
        }

        static AvroNode newNode(String name, Object value) {
            if (value instanceof GenericContainer container) {
                Schema schema = container.getSchema();
                Type valueType = schema.getType();
                return switch (valueType) {
                    case RECORD -> new RecordNode(name, (GenericData.Record) container);
                    case ARRAY -> new ArrayNode(name, (GenericData.Array<?>) container);
                    case FIXED, ENUM -> new ScalarNode(name, container);
                    default -> throw new RuntimeException("Unsupported Avro type: " + valueType);
                };
            }

            if (value instanceof Map<?, ?> map) {
                return new MapNode(name, map);
            }

            if (value == null) {
                return new NullValueNode(name);
            }

            return new ScalarNode(name, value);
        }
    }

    static class RecordNode implements AvroNode {

        private final GenericData.Record record;
        private final String name;

        RecordNode(String name, GenericData.Record record) {
            this.name = name;
            this.record = record;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            return record.hasField(name);
        }

        @Override
        public AvroNode get(String nodeName, String propertyName) {
            Object field = record.get(propertyName);
            return AvroNode.newNode(nodeName, field);
        }

        @Override
        public String text() {
            return record.toString();
        }

        public void flatIntoMap(Map<String, String> target) {
            for (Schema.Field field : record.getSchema().getFields()) {
                Object object = record.get(field.name());
                target.put(field.name(), Objects.toString(object, null));
            }
        }
    }

    static class ArrayNode implements AvroNode {

        private final GenericData.Array<?> array;
        private final String name;

        ArrayNode(String name, GenericData.Array<?> array) {
            this.name = name;
            this.array = array;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public int size() {
            return array.size();
        }

        @Override
        public AvroNode get(String nodeName, int index) {
            Object element = array.get(index);
            return AvroNode.newNode(nodeName, element);
        }

        @Override
        public String text() {
            return array.toString();
        }

        public void flatIntoMap(Map<String, String> target) {
            for (int i = 0; i < array.size(); i++) {
                target.put(name + "[" + i + "]", Objects.toString(array.get(i), null));
            }
        }
    }

    static class MapNode implements AvroNode {

        private final Map<?, ?> map;
        private final String name;

        MapNode(String name, Map<?, ?> map) {
            this.name = name;
            this.map = map;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            return map.containsKey(new Utf8(name));
        }

        @Override
        public AvroNode get(String nodeName, String propertyName) {
            Object value = map.get(new Utf8(propertyName));
            return AvroNode.newNode(nodeName, value);
        }

        @Override
        public String text() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    sb.append(", ");
                } else {
                    first = false;
                }
                sb.append(entry.getKey())
                        .append(": ")
                        .append(Objects.toString(entry.getValue(), null));
            }
            sb.append("}");
            return sb.toString();
        }

        public void flatIntoMap(Map<String, String> target) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                target.put(entry.getKey().toString(), Objects.toString(entry.getValue(), null));
            }
        }
    }

    static class ScalarNode implements AvroNode {

        private final Object value;
        private final String name;

        ScalarNode(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isScalar() {
            return true;
        }

        @Override
        public String text() {
            return value.toString();
        }
    }

    static class NullValueNode implements AvroNode {

        private final String name;

        NullValueNode(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean isScalar() {
            return true;
        }

        @Override
        public String text() {
            return null;
        }
    }

    private static class GenericRecordKeySelectorSupplier
            implements KeySelectorSupplier<GenericRecord> {

        private final Deserializer<GenericRecord> deserializer;

        GenericRecordKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = GenericRecordDeserializers.KeyDeserializer(config);
        }

        @Override
        public KeySelector<GenericRecord> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new GenericRecordKeySelector(expression);
        }

        @Override
        public Deserializer<GenericRecord> deserializer() {
            return deserializer;
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            return EvaluatorType.AVRO;
        }
    }

    private static final class GenericRecordKeySelector
            extends StructuredBaseSelector<GenericRecord, AvroNode>
            implements KeySelector<GenericRecord> {

        GenericRecordKeySelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, Constant.KEY, AvroNode::newNode);
        }

        @Override
        public Data extractKey(
                String name, KafkaRecord<GenericRecord, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(name, record::key, checkScalar);
        }

        @Override
        public Data extractKey(KafkaRecord<GenericRecord, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(record::key, checkScalar);
        }

        @Override
        public void extractKeyInto(KafkaRecord<GenericRecord, ?> record, Map<String, String> target)
                throws ValueException {
            evalInto(record::key, target);
        }
    }

    private static class GenericRecordValueSelectorSupplier
            implements ValueSelectorSupplier<GenericRecord> {

        private final Deserializer<GenericRecord> deserializer;

        GenericRecordValueSelectorSupplier(ConnectorConfig config) {
            this.deserializer = GenericRecordDeserializers.ValueDeserializer(config);
        }

        @Override
        public ValueSelector<GenericRecord> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new GenericRecordValueSelector(expression);
        }

        @Override
        public Deserializer<GenericRecord> deserializer() {
            return deserializer;
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            return EvaluatorType.AVRO;
        }
    }

    private static final class GenericRecordValueSelector
            extends StructuredBaseSelector<GenericRecord, AvroNode>
            implements ValueSelector<GenericRecord> {

        GenericRecordValueSelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, Constant.VALUE, AvroNode::newNode);
        }

        @Override
        public Data extractValue(
                String name, KafkaRecord<?, GenericRecord> record, boolean checkScalar)
                throws ValueException {
            return eval(name, record::value, checkScalar);
        }

        @Override
        public Data extractValue(KafkaRecord<?, GenericRecord> record, boolean checkScalar)
                throws ValueException {
            return eval(record::value, checkScalar);
        }

        @Override
        public void extractValueInto(
                KafkaRecord<?, GenericRecord> record, Map<String, String> target)
                throws ValueException {
            evalInto(record::value, target);
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
