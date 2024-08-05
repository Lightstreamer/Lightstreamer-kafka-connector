
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

import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaSinkRecord;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConnectSelectorsSuppliers {

    private static class SchemaAndValueNode implements Node<SchemaAndValueNode> {

        private final SchemaAndValue data;

        SchemaAndValueNode(SchemaAndValue data) {
            this.data = data;
        }

        @Override
        public boolean has(String name) {
            Schema schema = data.schema();
            return switch (schema.type()) {
                case STRUCT -> schema.field(name) != null;

                case MAP -> {
                    @SuppressWarnings("unchecked")
                    Map<String, ?> map = (Map<String, ?>) data.value();
                    yield map.containsKey(name);
                }

                default -> false;
            };
        }

        @Override
        public SchemaAndValueNode get(String name) {
            Schema schema = data.schema();

            SchemaAndValue schemaAndValue =
                    switch (schema.type()) {
                        case MAP -> {
                            Schema valueSchema = schema.valueSchema();
                            @SuppressWarnings("unchecked")
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

        @Override
        public boolean isArray() {
            return data.schema().type().equals(Schema.Type.ARRAY);
        }

        @Override
        public int size() {
            if (isArray()) {
                @SuppressWarnings("unchecked")
                List<Object> array = (List<Object>) data.value();
                return array.size();
            }
            return 0;
        }

        @Override
        public SchemaAndValueNode get(int index) {
            @SuppressWarnings("unchecked")
            List<Object> array = (List<Object>) data.value();
            Schema elementsSchema = data.schema().valueSchema();
            return new SchemaAndValueNode(new SchemaAndValue(elementsSchema, array.get(index)));
        }

        @Override
        public boolean isNull() {
            return data.value() == null;
        }

        @Override
        public boolean isScalar() {
            // Null is considered a scalar value
            if (data.value() == null) {
                return true;
            }
            return data.schema().type().isPrimitive();
        }

        @Override
        public String asText(String defaultStr) {
            Object value = data.value();
            if (value != null) {
                if (value instanceof ByteBuffer buffer) {
                    return Arrays.toString(buffer.array());
                } else if (value instanceof byte[] bt) {
                    return Arrays.toString(bt);
                } else {
                    return value.toString();
                }
            }
            return defaultStr;
        }
    }

    private static class ConnectKeySelectorSupplierImpl implements ConnectKeySelectorSupplier {

        ConnectKeySelectorSupplierImpl() {}

        @Override
        public ConnectKeySelector newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new ConnectKeySelectorImpl(name, expression);
        }
    }

    private static class ConnectKeySelectorImpl extends StructuredBaseSelector<SchemaAndValueNode>
            implements ConnectKeySelector {

        ConnectKeySelectorImpl(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.KEY, false);
        }

        @Override
        public Data extractKey(KafkaRecord<Object, ?> record) {
            return super.eval(asNode((KafkaSinkRecord) record));
        }

        private SchemaAndValueNode asNode(KafkaRecord.KafkaSinkRecord sinkRecord) {
            return new SchemaAndValueNode(
                    new SchemaAndValue(sinkRecord.keySchema(), sinkRecord.key()));
        }
    }

    private static class ConnectValueSelectorSupplierImpl implements ConnectValueSelectorSupplier {

        @Override
        public ConnectValueSelector newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new ConnectValueSelectorImpl(name, expression);
        }
    }

    private static class ConnectValueSelectorImpl extends StructuredBaseSelector<SchemaAndValueNode>
            implements ConnectValueSelector {

        ConnectValueSelectorImpl(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.VALUE, false);
        }

        @Override
        public Data extractValue(KafkaRecord<?, Object> record) {
            return eval(asNode((KafkaSinkRecord) record));
        }

        private SchemaAndValueNode asNode(KafkaRecord.KafkaSinkRecord sinkRecord) {
            return new SchemaAndValueNode(
                    new SchemaAndValue(sinkRecord.valueSchema(), sinkRecord.value()));
        }
    }

    public static ConnectKeySelectorSupplier keySelectorSupplier() {
        return new ConnectKeySelectorSupplierImpl();
    }

    public static ConnectValueSelectorSupplier valueSelectorSupplier() {
        return new ConnectValueSelectorSupplierImpl();
    }
}
