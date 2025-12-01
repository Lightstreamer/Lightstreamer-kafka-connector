
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

import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements selector suppliers for Protocol Buffers DynamicMessage objects in Kafka records.
 *
 * <p>This class provides the infrastructure to create selectors that can extract data from Protocol
 * Buffers DynamicMessage objects stored in Kafka record keys and values. The extraction is based on
 * path expressions that allow navigation through the hierarchical structure of Protocol Buffer
 * messages.
 *
 * <p>The class implements a navigable tree structure for Protocol Buffer messages:
 *
 * <ul>
 *   <li>Regular message fields are accessed through the {@link MessageWrapperNode}
 *   <li>Repeated fields (arrays) are handled by {@link RepeatedFieldNode}
 *   <li>Map fields are processed by {@link MapFieldNode}
 *   <li>Scalar values are represented by {@link ScalarFieldNode}
 * </ul>
 *
 * <p>The selector suppliers created by this class will provide selectors that:
 *
 * <ul>
 *   <li>Can navigate through the Protocol Buffer message structure
 *   <li>Extract values based on configured expressions
 *   <li>Convert the extracted values to the appropriate representation
 * </ul>
 *
 * <p>
 *
 * @see com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker
 * @see com.google.protobuf.DynamicMessage
 */
public class DynamicMessageSelectorSuppliers
        implements KeyValueSelectorSuppliersMaker<DynamicMessage> {

    /**
     * Represents a node in a Protocol Buffer message structure.
     *
     * <p>This interface provides a tree-like navigation model for Protobuf messages, allowing
     * hierarchical access to fields, nested messages, and repeated elements. Default
     * implementations are provided for most methods, which subclasses can override based on their
     * specific node type.
     *
     * <p>The interface supports:
     *
     * <ul>
     *   <li>Property access via names
     *   <li>Array/repeated field access via indices
     *   <li>Type checking capabilities
     *   <li>Value extraction
     * </ul>
     *
     * <p>Use the {@link #newNode(Object, FieldDescriptor)} factory method to create appropriate
     * node implementations based on the Protobuf field type.
     */
    interface ProtobufNode extends Node<ProtobufNode> {

        @Override
        default ProtobufNode get(String nodeName, String propertyName) {
            return NullNode.INSTANCE;
        }

        @Override
        default boolean has(String propertyname) {
            return false;
        }

        @Override
        default ProtobufNode get(String nodeName, int index) {
            return NullNode.INSTANCE;
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

        static class NullNode implements ProtobufNode {

            private static final NullNode INSTANCE = new NullNode();

            @Override
            public String name() {
                return "null";
            }

            @Override
            public String text() {
                return "null";
            }

            @Override
            public boolean isNull() {
                return true;
            }
        }

        /**
         * Creates a new {@link ProtobufNode} instance based on the provided value and field
         * descriptor.
         *
         * @param nodeName the name to assign to the created node
         * @param value the value to be wrapped in a {@link ProtobufNode}
         * @param fieldDescriptor the descriptor of the field which provides type information
         * @return a new {@link ProtobufNode} instance, either a {@link MessageWrapperNode} if the
         *     field type is {@code MESSAGE}, or a {@link ScalarFieldNode} for all other field types
         */
        static ProtobufNode newNode(
                String nodeName, Object value, FieldDescriptor fieldDescriptor) {
            Type type = fieldDescriptor.getType();
            return switch (type) {
                case MESSAGE -> new MessageWrapperNode(nodeName, (Message) value);
                default -> new ScalarFieldNode(nodeName, value, fieldDescriptor);
            };
        }
    }

    /**
     * Implementation of the {@link ProtobufNode} interface for Protocol Buffer Message objects.
     *
     * <p>This class wraps a Protocol Buffer {@link Message} and provides structured access to its
     * fields. It handles different field types, including map fields and repeated fields, by
     * wrapping them in appropriate node implementations.
     */
    static class MessageWrapperNode implements ProtobufNode {

        private final Message message;
        private final Descriptor descriptor;
        private final String name;

        MessageWrapperNode(String name, Message message) {
            this.message = message;
            this.name = name;
            this.descriptor = message.getDescriptorForType();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String name) {
            return descriptor.findFieldByName(name) != null;
        }

        @Override
        public ProtobufNode get(String nodeName, String propertyName) {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(propertyName);
            if (fieldDescriptor.isMapField()) {
                return new MapFieldNode(nodeName, message, fieldDescriptor);
            }
            if (fieldDescriptor.isRepeated()) {
                return new RepeatedFieldNode(nodeName, message, fieldDescriptor);
            }

            Object value = message.getField(fieldDescriptor);
            return ProtobufNode.newNode(nodeName, value, fieldDescriptor);
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
                Object value = message.getField(fieldDescriptor);
                target.put(
                        fieldDescriptor.getName(),
                        TextFormat.printer().printFieldToString(fieldDescriptor, value));
            }
        }

        @Override
        public String text() {
            return message.toString();
        }
    }

    /**
     * Implementation of {@link ProtobufNode} that handles repeated fields in Protocol Buffers
     * messages.
     *
     * <p>A repeated field in Protocol Buffers is similar to an array, containing multiple values of
     * the same type. This class provides access to the elements within the repeated field and
     * allows traversal through the repeated field structure.
     *
     * <p>This node always returns true for {@link #isArray()} and provides methods to access the
     * number of elements and retrieve individual nodes at specific indices.
     */
    static class RepeatedFieldNode implements ProtobufNode {

        private final String name;
        private final Message containing;
        private final FieldDescriptor fieldDescriptor;

        RepeatedFieldNode(String name, Message containing, FieldDescriptor fieldDescriptor) {
            this.name = name;
            this.containing = containing;
            this.fieldDescriptor = fieldDescriptor;
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
            return containing.getRepeatedFieldCount(fieldDescriptor);
        }

        @Override
        public ProtobufNode get(String nodeName, int index) {
            Object value = containing.getRepeatedField(fieldDescriptor, index);
            return ProtobufNode.newNode(nodeName, value, fieldDescriptor);
        }

        @Override
        public String text() {
            return TextFormat.printer()
                    .printFieldToString(fieldDescriptor, containing.getField(fieldDescriptor));
        }
    }

    /**
     * Implementation of {@link ProtobufNode} that handles Protocol Buffers map fields.
     *
     * <p>This class provides access to entries of a map field in a Protocol Buffers message. It
     * extracts the map entries from the container message and allows access to values by key
     * through the {@link ProtobufNode} interface.
     *
     * @see ProtobufNode
     */
    static class MapFieldNode implements ProtobufNode {

        private final String name;
        private final Message containing;
        private final Map<String, Object> map = new HashMap<>();
        private final FieldDescriptor fieldValueDescriptor;
        private final FieldDescriptor fieldDescriptor;

        MapFieldNode(String name, Message containing, FieldDescriptor fieldDescriptor) {
            this.name = name;
            this.containing = containing;
            this.fieldDescriptor = fieldDescriptor;

            @SuppressWarnings("unchecked")
            List<MapEntry<?, ?>> entries =
                    (List<MapEntry<?, ?>>) containing.getField(fieldDescriptor);
            for (MapEntry<?, ?> entry : entries) {
                map.put(entry.getKey().toString(), entry.getValue());
            }

            this.fieldValueDescriptor = fieldDescriptor.getMessageType().findFieldByName("value");
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String propertyname) {
            return map.containsKey(propertyname);
        }

        @Override
        public ProtobufNode get(String nodeName, String propertyName) {
            Object value = map.get(propertyName);
            return ProtobufNode.newNode(nodeName, value, fieldValueDescriptor);
        }

        @Override
        public String text() {
            return TextFormat.printer()
                    .printFieldToString(fieldDescriptor, containing.getField(fieldDescriptor));
        }
    }

    /**
     * Represents a scalar (primitive) value node in a Protocol Buffer message structure. This class
     * handles individual scalar values extracted from a Protobuf message, along with their
     * associated field descriptors.
     *
     * <p>This implementation provides methods to determine node type and convert the scalar value
     * to a string representation.
     *
     * <p>
     *
     * @implSpec For {@code BYTES} type fields, the value is converted from {@link ByteString} to
     *     UTF-8 string when requesting text representation.
     */
    static class ScalarFieldNode implements ProtobufNode {

        private final String name;
        private final Object value;
        private final FieldDescriptor fieldDescriptor;

        ScalarFieldNode(String name, Object value, FieldDescriptor fieldDescriptor) {
            this.name = name;
            this.value = value;
            this.fieldDescriptor = fieldDescriptor;
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
            if (fieldDescriptor.getType().equals(Type.BYTES)) {
                return ((ByteString) value).toStringUtf8();
            }
            return value.toString();
        }
    }

    /**
     * A supplier implementation for creating key selectors that work with Protocol Buffers'
     * DynamicMessage objects.
     *
     * <p>This class provides the means to create key selectors for extracting data from
     * DynamicMessage values in Kafka records. It maintains a deserializer instance that can convert
     * Kafka record keys into DynamicMessage objects.
     */
    private static class DynamicMessageKeySelectorSupplier
            implements KeySelectorSupplier<DynamicMessage> {

        private final Deserializer<DynamicMessage> deserializer;

        DynamicMessageKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = DynamicMessageDeserializers.KeyDeserializer(config);
        }

        @Override
        public KeySelector<DynamicMessage> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new DynamicMessageKeySelector(expression);
        }

        @Override
        public Deserializer<DynamicMessage> deserializer() {
            return deserializer;
        }
    }

    /**
     * A selector implementation for extracting key values from Protobuf DynamicMessage objects.
     *
     * <p>This class extends {@link StructuredBaseSelector} to provide functionality for selecting
     * and extracting data from Protobuf DynamicMessage keys in Kafka records according to the
     * provided expression.
     *
     * @see StructuredBaseSelector
     * @see KeySelector
     * @see DynamicMessage
     */
    private static final class DynamicMessageKeySelector
            extends StructuredBaseSelector<ProtobufNode> implements KeySelector<DynamicMessage> {

        DynamicMessageKeySelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, Constant.KEY);
        }

        @Override
        public Data extractKey(
                String name, KafkaRecord<DynamicMessage, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(
                    name,
                    record::key,
                    (rootName, key) -> new MessageWrapperNode(rootName, (Message) key),
                    checkScalar);
        }

        @Override
        public Data extractKey(KafkaRecord<DynamicMessage, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(
                    record::key,
                    (rootName, key) -> new MessageWrapperNode(rootName, (Message) key),
                    checkScalar);
        }

        @Override
        public void extractKeyInto(
                KafkaRecord<DynamicMessage, ?> record, Map<String, String> target)
                throws ValueException {
            evalInto(
                    record::key,
                    (rootName, key) -> new MessageWrapperNode(rootName, (Message) key),
                    target);
        }
    }

    /**
     * A supplier implementation for creating value selectors that work with Protocol Buffers'
     * DynamicMessage objects.
     *
     * <p>This class provides the means to create value selectors for extracting data from
     * DynamicMessage values in Kafka records. It maintains a deserializer instance that can convert
     * Kafka record values into DynamicMessage objects.
     */
    private static class DynamicMessageValueSelectorSupplier
            implements ValueSelectorSupplier<DynamicMessage> {

        private final Deserializer<DynamicMessage> deserializer;

        DynamicMessageValueSelectorSupplier(ConnectorConfig config) {
            this.deserializer = DynamicMessageDeserializers.ValueDeserializer(config);
        }

        @Override
        public ValueSelector<DynamicMessage> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new DynamicMessageValueSelector(expression);
        }

        @Override
        public Deserializer<DynamicMessage> deserializer() {
            return deserializer;
        }
    }

    /**
     * A selector implementation for extracting data from Protocol Buffers DynamicMessage values in
     * Kafka records.
     *
     * <p>This selector operates specifically on the value portion of a Kafka record when the value
     * is a Protocol Buffers DynamicMessage. It creates a structured representation of the message
     * and delegates to the base extraction functionality.
     *
     * @see StructuredBaseSelector
     * @see ValueSelector
     * @see ProtobufNode
     * @see DynamicMessage
     */
    private static final class DynamicMessageValueSelector
            extends StructuredBaseSelector<ProtobufNode> implements ValueSelector<DynamicMessage> {

        DynamicMessageValueSelector(ExtractionExpression expression) throws ExtractionException {
            super(expression, Constant.VALUE);
        }

        @Override
        public Data extractValue(
                String name, KafkaRecord<?, DynamicMessage> record, boolean checkScalar)
                throws ValueException {
            return eval(
                    name,
                    record::value,
                    (rootName, value) -> new MessageWrapperNode(rootName, (Message) value),
                    checkScalar);
        }

        @Override
        public Data extractValue(KafkaRecord<?, DynamicMessage> record, boolean checkScalar)
                throws ValueException {
            return eval(
                    record::value,
                    (rootName, value) -> new MessageWrapperNode(rootName, (Message) value),
                    checkScalar);
        }

        @Override
        public void extractValueInto(
                KafkaRecord<?, DynamicMessage> record, Map<String, String> target)
                throws ValueException {
            evalInto(
                    record::value,
                    (rootName, value) -> new MessageWrapperNode(rootName, (Message) value),
                    target);
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
