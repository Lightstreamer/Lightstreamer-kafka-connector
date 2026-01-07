
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

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;

import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaSinkRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorEvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConnectSelectorsSuppliers implements KeyValueSelectorSuppliersMaker<Object> {

    private static final JsonConverter JSON_CONVERTER;

    static {
        JSON_CONVERTER = new JsonConverter();
        JSON_CONVERTER.configure(
                Map.of(
                        JsonConverterConfig.TYPE_CONFIG,
                        "key",
                        JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
                        "false"));
    }

    interface ConnectNode extends Node<ConnectNode> {

        static ConnectNode newNode(String name, Schema schema, Object value) {
            if (schema != null && value != null) {
                return switch (schema.type()) {
                    case STRUCT -> new ConnectStructNode(name, (Struct) value);
                    case ARRAY -> {
                        @SuppressWarnings("unchecked")
                        List<Object> array = (List<Object>) value;
                        yield new ConnectArrayNode(name, schema.valueSchema(), array);
                    }
                    case MAP -> {
                        @SuppressWarnings("unchecked")
                        Map<String, ?> map = (Map<String, ?>) value;
                        yield new ConnectMapNode(name, schema.valueSchema(), map);
                    }
                    default -> new ConnectScalarNode(name, value);
                };
            }
            if (value == null) {
                return new ConnectNullNode(name);
            }
            throw ValueException.nonSchemaAssociated();
        }

        static String textValue(Object value) {
            if (value != null) {
                if (value instanceof Struct struct) {
                    byte[] fromConnectData =
                            JSON_CONVERTER.fromConnectData(null, struct.schema(), struct);
                    return new String(fromConnectData);
                } else if (value instanceof ByteBuffer buffer) {
                    return Arrays.toString(buffer.array());
                } else if (value instanceof byte[] bt) {
                    return Arrays.toString(bt);
                } else if (value instanceof Map<?, ?> map) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("{");
                    boolean first = true;
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (!first) {
                            sb.append(", ");
                        } else {
                            first = false;
                        }
                        sb.append(entry.getKey()).append(": ").append(textValue(entry.getValue()));
                    }
                    sb.append("}");
                    return sb.toString();
                } else {
                    return value.toString();
                }
            }
            return null;
        }
    }

    private static class ConnectNullNode extends Node.NullNode<ConnectNode> implements ConnectNode {

        ConnectNullNode(String name) {
            super(name);
        }
    }

    private static class ConnectStructNode implements ConnectNode {

        private final String name;
        private final Struct struct;
        private final Schema schema;

        ConnectStructNode(String name, Struct struct) {
            this.name = name;
            this.struct = struct;
            this.schema = struct.schema();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            return false;
        }

        @Override
        public ConnectNode getProperty(String nodeName, String propertyName) {
            Field field = schema.field(propertyName);
            if (field != null) {
                Object value = struct.get(field);
                return ConnectNode.newNode(nodeName, field.schema(), value);
            }
            throw ValueException.fieldNotFound(propertyName);
        }

        @Override
        public boolean isArray() {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public ConnectStructNode getIndexed(
                String nodeName, int index, String indexedPropertyName) {
            throw ValueException.nonArrayObject(index);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            for (Field field : schema.fields()) {
                target.put(field.name(), ConnectNode.textValue(struct.get(field)));
            }
        }

        @Override
        public String text() {
            return ConnectNode.textValue(struct);
        }
    }

    static class ConnectArrayNode implements ConnectNode {

        private final String name;
        private final List<Object> array;
        private final int size;
        private final Schema valueSchema;

        ConnectArrayNode(String name, Schema valueSchema, List<Object> array) {
            this.name = name;
            this.array = array;
            this.size = array.size();
            this.valueSchema = valueSchema;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            return false;
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public ConnectNode getProperty(String nodeName, String propertyName) {
            throw ValueException.arrayObject(propertyName);
        }

        @Override
        public ConnectNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            if (index < size) {
                Object value = array.get(index);
                return ConnectNode.newNode(nodeName, valueSchema, value);
            }
            throw ValueException.indexOfOutBounds(index);
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            // Pre-allocate StringBuilder for array index keys to avoid string concatenation
            StringBuilder keyBuilder =
                    new StringBuilder(name.length() + 4); // reasonable initial capacity
            for (int i = 0; i < this.size; i++) {
                String value = ConnectNode.textValue(array.get(i));
                keyBuilder.append(name);
                keyBuilder.append('[').append(i).append(']');
                target.put(keyBuilder.toString(), value);
                keyBuilder.delete(0, keyBuilder.length()); // reset for next use
            }
        }

        @Override
        public String text() {
            return ConnectNode.textValue(array);
        }
    }

    static class ConnectMapNode implements ConnectNode {

        private final String name;
        private final Map<String, ?> map;
        private final Schema valueSchema;

        ConnectMapNode(String name, Schema valueSchema, Map<String, ?> map) {
            this.name = name;
            this.map = map;
            this.valueSchema = valueSchema;
        }

        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            return false;
        }

        @Override
        public boolean isArray() {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public ConnectNode getProperty(String nodeName, String propertyName) {
            Object value = map.get(propertyName);
            if (value != null) {
                return ConnectNode.newNode(nodeName, valueSchema, value);
            }
            throw ValueException.fieldNotFound(propertyName);
        }

        @Override
        public ConnectNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            throw ValueException.nonArrayObject(index);
        }

        public void flatIntoMap(Map<String, String> target) {
            for (Map.Entry<String, ?> entry : map.entrySet()) {
                String key = entry.getKey();
                target.put(key, ConnectNode.textValue(entry.getValue()));
            }
        }

        @Override
        public String text() {
            return ConnectNode.textValue(map);
        }
    }

    static class ConnectScalarNode implements ConnectNode {

        private final Object value;
        private final String name;

        ConnectScalarNode(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            return false;
        }

        @Override
        public boolean isArray() {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public boolean isScalar() {
            return true;
        }

        @Override
        public ConnectScalarNode getProperty(String nodeName, String propertyName) {
            throw ValueException.scalarObject(propertyName);
        }

        @Override
        public ConnectScalarNode getIndexed(
                String nodeName, int index, String indexedPropertyName) {
            throw ValueException.noIndexedField(indexedPropertyName);
        }

        @Override
        public String text() {
            return ConnectNode.textValue(value);
        }
    }

    private static class ConnectKeySelectorSupplier implements KeySelectorSupplier<Object> {

        ConnectKeySelectorSupplier() {}

        @Override
        public KeySelector<Object> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new ConnectKeySelector(expression);
        }

        @Override
        public Deserializer<Object> deserializer() {
            throw new UnsupportedOperationException("Unimplemented method 'deserializer'");
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            return () -> "Struct";
        }
    }

    private static class ConnectKeySelector
            extends StructuredBaseSelector<SchemaAndValue, ConnectNode>
            implements KeySelector<Object> {

        ConnectKeySelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, KEY, ConnectKeySelector::asNode);
        }

        @Override
        public Data extractKey(String name, KafkaRecord<Object, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(name, () -> ((KafkaSinkRecord) record).keySchemaAndValue(), checkScalar);
        }

        @Override
        public Data extractKey(KafkaRecord<Object, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(() -> ((KafkaSinkRecord) record).keySchemaAndValue(), checkScalar);
        }

        private static ConnectNode asNode(String name, SchemaAndValue schemaAndValue) {
            return ConnectNode.newNode(name, schemaAndValue.schema(), schemaAndValue.value());
        }

        @Override
        public void extractKeyInto(KafkaRecord<Object, ?> record, Map<String, String> target)
                throws ValueException {
            evalInto(() -> ((KafkaSinkRecord) record).keySchemaAndValue(), target);
        }
    }

    private static class ConnectValueSelectorSupplier implements ValueSelectorSupplier<Object> {

        @Override
        public ValueSelector<Object> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new ConnectValueSelector(expression);
        }

        @Override
        public Deserializer<Object> deserializer() {
            throw new UnsupportedOperationException("Unimplemented method 'deserializer'");
        }

        @Override
        public SelectorEvaluatorType evaluatorType() {
            return () -> "Struct";
        }
    }

    private static class ConnectValueSelector
            extends StructuredBaseSelector<SchemaAndValue, ConnectNode>
            implements ValueSelector<Object> {

        ConnectValueSelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, VALUE, ConnectValueSelector::asNode);
        }

        @Override
        public Data extractValue(String name, KafkaRecord<?, Object> record, boolean checkScalar)
                throws ValueException {
            return eval(name, () -> ((KafkaSinkRecord) record).valueSchemaAndValue(), checkScalar);
        }

        @Override
        public Data extractValue(KafkaRecord<?, Object> record, boolean checkScalar)
                throws ValueException {
            return eval(() -> ((KafkaSinkRecord) record).valueSchemaAndValue(), checkScalar);
        }

        private static ConnectNode asNode(String name, SchemaAndValue schemaAndValue) {
            return ConnectNode.newNode(name, schemaAndValue.schema(), schemaAndValue.value());
        }

        @Override
        public void extractValueInto(KafkaRecord<?, Object> record, Map<String, String> target)
                throws ValueException {
            evalInto(() -> ((KafkaSinkRecord) record).valueSchemaAndValue(), target);
        }
    }

    @Override
    public KeySelectorSupplier<Object> makeKeySelectorSupplier() {
        return new ConnectKeySelectorSupplier();
    }

    @Override
    public ValueSelectorSupplier<Object> makeValueSelectorSupplier() {
        return new ConnectValueSelectorSupplier();
    }

    public KeyValueSelectorSuppliers<Object, Object> makeKeyValueSelectorSuppliers() {
        return KeyValueSelectorSuppliers.of(makeKeySelectorSupplier(), makeValueSelectorSupplier());
    }
}
