
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

import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaSinkRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliers;
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

public class ConnectSelectorsSuppliers implements KeyValueSelectorSuppliers<Object, Object> {

    private static class SchemaAndValueNode implements Node<SchemaAndValueNode> {

        private static final JsonConverter jsonConverter;

        static {
            jsonConverter = new JsonConverter();
            jsonConverter.configure(
                    Map.of(
                            JsonConverterConfig.TYPE_CONFIG,
                            "key",
                            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
                            "false"));
        }

        private final SchemaAndValue data;
        private final String name;

        SchemaAndValueNode(String name, SchemaAndValue data) {
            this.name = name;
            this.data = data;
            if (data.schema() == null) {
                throw ValueException.nonSchemaAssociated();
            }
        }

        @Override
        public String name() {
            return name;
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
        public SchemaAndValueNode get(String nodeName, String propertyName) {
            Schema schema = data.schema();

            SchemaAndValue schemaAndValue =
                    switch (schema.type()) {
                        case MAP -> {
                            Schema valueSchema = schema.valueSchema();
                            @SuppressWarnings("unchecked")
                            Map<String, ?> map = (Map<String, ?>) data.value();
                            yield new SchemaAndValue(valueSchema, map.get(propertyName));
                        }

                        default -> {
                            Struct struct = (Struct) data.value();
                            Field field = schema.field(propertyName);
                            yield new SchemaAndValue(field.schema(), struct.get(field));
                        }
                    };
            return new SchemaAndValueNode(nodeName, schemaAndValue);
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
        public SchemaAndValueNode get(String nodeName, int index) {
            @SuppressWarnings("unchecked")
            List<Object> array = (List<Object>) data.value();
            Schema elementsSchema = data.schema().valueSchema();
            return new SchemaAndValueNode(
                    name + "[" + index + "]", new SchemaAndValue(elementsSchema, array.get(index)));
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
        public String text() {
            Object value = data.value();
            if (value != null) {
                if (value instanceof Struct struct) {
                    byte[] fromConnectData =
                            jsonConverter.fromConnectData(null, struct.schema(), struct);
                    return new String(fromConnectData);
                } else if (value instanceof ByteBuffer buffer) {
                    return Arrays.toString(buffer.array());
                } else if (value instanceof byte[] bt) {
                    return Arrays.toString(bt);
                } else {
                    return value.toString();
                }
            }
            return null;
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
            extends StructuredBaseSelector<KafkaSinkRecord, SchemaAndValueNode>
            implements KeySelector<Object> {

        ConnectKeySelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, Constant.KEY, ConnectKeySelector::asNode);
        }

        @Override
        public Data extractKey(String name, KafkaRecord<Object, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(name, () -> ((KafkaSinkRecord) record), checkScalar);
        }

        @Override
        public Data extractKey(KafkaRecord<Object, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(() -> ((KafkaSinkRecord) record), checkScalar);
        }

        private static SchemaAndValueNode asNode(String name, KafkaSinkRecord sinkRecord) {
            return new SchemaAndValueNode(
                    name, new SchemaAndValue(sinkRecord.keySchema(), sinkRecord.key()));
        }

        @Override
        public void extractKeyInto(KafkaRecord<Object, ?> record, Map<String, String> target)
                throws ValueException {}
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
            extends StructuredBaseSelector<KafkaSinkRecord, SchemaAndValueNode>
            implements ValueSelector<Object> {

        ConnectValueSelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, Constant.VALUE, ConnectValueSelector::asNode);
        }

        @Override
        public Data extractValue(String name, KafkaRecord<?, Object> record, boolean checkScalar)
                throws ValueException {
            return eval(name, () -> ((KafkaSinkRecord) record), checkScalar);
        }

        @Override
        public Data extractValue(KafkaRecord<?, Object> record, boolean checkScalar)
                throws ValueException {
            return eval(() -> ((KafkaSinkRecord) record), checkScalar);
        }

        private static SchemaAndValueNode asNode(String name, KafkaSinkRecord sinkRecord) {
            return new SchemaAndValueNode(
                    name, new SchemaAndValue(sinkRecord.valueSchema(), sinkRecord.value()));
        }

        @Override
        public void extractValueInto(KafkaRecord<?, Object> record, Map<String, String> target)
                throws ValueException {
            evalInto(() -> ((KafkaSinkRecord) record), target);
        }
    }

    private final ConnectKeySelectorSupplier keySelectorSupplier = new ConnectKeySelectorSupplier();
    private final ConnectValueSelectorSupplier valueSelectorSupplier =
            new ConnectValueSelectorSupplier();

    @Override
    public KeySelectorSupplier<Object> keySelectorSupplier() {
        return keySelectorSupplier;
    }

    @Override
    public ValueSelectorSupplier<Object> valueSelectorSupplier() {
        return valueSelectorSupplier;
    }
}
