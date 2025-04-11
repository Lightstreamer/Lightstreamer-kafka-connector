
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

package com.lightstreamer.kafka.adapters.mapping.selectors.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
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

import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicMessageSelectorSuppliers
        implements KeyValueSelectorSuppliersMaker<DynamicMessage> {

    interface ProtoNode extends Node<ProtoNode> {

        default boolean has(String propertyname) {
            return false;
        }

        default ProtoNode get(String propertyname) {
            return null;
        }

        default boolean isArray() {
            return false;
        }

        default int size() {
            return 0;
        }

        default ProtoNode get(int index) {
            return null;
        }

        default boolean isNull() {
            return false;
        }

        default boolean isScalar() {
            return false;
        }

        String asText();

        static ProtoNode newNode(Object value, FieldDescriptor fieldDescriptor) {
            Type type = fieldDescriptor.getType();
            return switch (type) {
                case MESSAGE -> new MessageNode((Message) value);
                default -> new ScalarNode(value, fieldDescriptor);
            };
        }
    }

    interface NonScalerNode extends ProtoNode {
        default boolean isScalar() {
            return false;
        }
    }

    static class MessageNode implements ProtoNode {
        private final Message message;
        private Descriptor descriptor;

        MessageNode(Message message) {
            this.message = message;
            this.descriptor = message.getDescriptorForType();
        }

        @Override
        public boolean has(String name) {
            return descriptor.findFieldByName(name) != null;
        }

        @Override
        public ProtoNode get(String name) {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(name);
            if (fieldDescriptor.isMapField()) {
                return new MapNode(message, fieldDescriptor);
            }
            if (fieldDescriptor.isRepeated()) {
                return new RepeatedNode(message, fieldDescriptor);
            }

            Object value = message.getField(fieldDescriptor);
            return ProtoNode.newNode(value, fieldDescriptor);
        }

        @Override
        public String asText() {
            return message.toString();
        }
    }

    static class RepeatedNode implements ProtoNode {
        private final Message container;
        private final FieldDescriptor fieldDescriptor;

        RepeatedNode(Message container, FieldDescriptor fieldDescriptor) {
            this.container = container;
            this.fieldDescriptor = fieldDescriptor;
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public int size() {
            return container.getRepeatedFieldCount(fieldDescriptor);
        }

        @Override
        public ProtoNode get(int index) {
            Object value = container.getRepeatedField(fieldDescriptor, index);
            return ProtoNode.newNode(value, fieldDescriptor);
        }

        @Override
        public String asText() {
            return TextFormat.printer()
                    .printFieldToString(fieldDescriptor, container.getField(fieldDescriptor));
        }
    }

    static class MapNode implements ProtoNode {
        private final Message container;
        private final Map<String, Object> map = new HashMap<>();
        private final FieldDescriptor fieldValueDescriptor;
        private FieldDescriptor fieldDescriptor;

        MapNode(Message container, FieldDescriptor fieldDescriptor) {
            this.container = container;
            this.fieldDescriptor = fieldDescriptor;

            @SuppressWarnings("unchecked")
            List<MapEntry<?, ?>> entries =
                    (List<MapEntry<?, ?>>) container.getField(fieldDescriptor);
            for (MapEntry<?, ?> entry : entries) {
                map.put(entry.getKey().toString(), entry.getValue());
            }

            this.fieldValueDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
        }

        @Override
        public boolean has(String propertyname) {
            return map.containsKey(propertyname);
        }

        @Override
        public ProtoNode get(String propertyname) {
            Object value = map.get(propertyname);
            return ProtoNode.newNode(value, fieldValueDescriptor);
        }

        @Override
        public String asText() {
            return TextFormat.printer()
                    .printFieldToString(fieldDescriptor, container.getField(fieldDescriptor));
        }
    }

    static class ScalarNode implements ProtoNode {
        private final Object value;
        private final FieldDescriptor fieldDescriptor;

        ScalarNode(Object value, FieldDescriptor fieldDescriptor) {
            this.value = value;
            this.fieldDescriptor = fieldDescriptor;
        }

        @Override
        public boolean isScalar() {
            return true;
        }

        @Override
        public String asText() {
            if (fieldDescriptor.getType().equals(Type.BYTES)) {
                return ((ByteString) value).toStringUtf8();
            }
            return value.toString();
        }
    }

    private static class DynamicMessageKeySelectorSupplier
            implements KeySelectorSupplier<DynamicMessage> {

        private final Deserializer<DynamicMessage> deserializer;

        DynamicMessageKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = DynamicRecordDeserializers.KeyDeserializer(config);
        }

        @Override
        public KeySelector<DynamicMessage> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new DynamicMessageKeySelector(name, expression);
        }

        @Override
        public Deserializer<DynamicMessage> deserializer() {
            return deserializer;
        }
    }

    private static final class DynamicMessageKeySelector extends StructuredBaseSelector<ProtoNode>
            implements KeySelector<DynamicMessage> {

        DynamicMessageKeySelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.KEY);
        }

        @Override
        public Data extractKey(KafkaRecord<DynamicMessage, ?> record, boolean checkScalar)
                throws ValueException {
            MessageNode node = new MessageNode(record.key());
            return super.eval(node, checkScalar);
        }
    }

    private static class DynamicMessageValueSelectorSupplier
            implements ValueSelectorSupplier<DynamicMessage> {

        private final Deserializer<DynamicMessage> deserializer;

        DynamicMessageValueSelectorSupplier(ConnectorConfig config) {
            this.deserializer = DynamicRecordDeserializers.ValueDeserializer(config);
        }

        @Override
        public ValueSelector<DynamicMessage> newSelector(
                String name, ExtractionExpression expression) throws ExtractionException {
            return new DynamicMessageValueSelector(name, expression);
        }

        @Override
        public Deserializer<DynamicMessage> deserializer() {
            return deserializer;
        }
    }

    private static final class DynamicMessageValueSelector extends StructuredBaseSelector<ProtoNode>
            implements ValueSelector<DynamicMessage> {

        DynamicMessageValueSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.VALUE);
        }

        @Override
        public Data extractValue(KafkaRecord<?, DynamicMessage> record, boolean checkScalar)
                throws ValueException {
            MessageNode node = new MessageNode(record.value());
            return super.eval(node, checkScalar);
        }
    }

    private final ConnectorConfig config;

    public DynamicMessageSelectorSuppliers(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public KeySelectorSupplier<DynamicMessage> makeKeySelectorSupplier() {
        return new DynamicMessageKeySelectorSupplier(config);
    }

    @Override
    public ValueSelectorSupplier<DynamicMessage> makeValueSelectorSupplier() {
        return new DynamicMessageValueSelectorSupplier(config);
    }
}
