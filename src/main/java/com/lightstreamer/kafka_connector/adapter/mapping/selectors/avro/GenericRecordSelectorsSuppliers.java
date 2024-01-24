package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import java.util.List;
import java.util.Objects;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

public class GenericRecordSelectorsSuppliers {

    public static KeySelectorSupplier<GenericRecord> keySelectorSupplier(ConnectorConfig config) {
        return new GenericRecordKeySelectorSupplier(config);
    }

    public static ValueSelectorSupplier<GenericRecord> valueSelectorSupplier(ConnectorConfig config) {
        return new GenericRecordValueSelectorSupplier(config);
    }

    static class GenericRecordBaseSelector extends BaseSelector {

        private static class FieldGetter implements NodeEvaluator<GenericRecord, Object> {

            private final String name;

            private FieldGetter(String name) {
                this.name = name;
            }

            @Override
            public Object get(GenericRecord record) {
                if (!record.hasField(name)) {
                    ValueException.throwFieldNotFound(name);
                }
                return record.get(name);
            }

        }

        private static class ArrayGetter implements NodeEvaluator<GenericRecord, Object> {

            private final String name;

            private final FieldGetter getter;

            private final List<Integer> indexes;

            ArrayGetter(String fieldName, List<Integer> indexes) {
                this.name = Objects.requireNonNull(fieldName);
                this.indexes = Objects.requireNonNull(indexes);
                this.getter = new FieldGetter(name);
            }

            @Override
            public Object get(GenericRecord record) {
                Object value = getter.get(record);
                for (int i : indexes) {
                    if (value instanceof GenericData.Array<?> array) {
                        value = array.get(i);
                    } else {
                        ValueException.throwNoIndexedField();
                    }
                }
                return value;
            }
        }

        private LinkedNode<NodeEvaluator<GenericRecord, Object>> linkedNode;

        private static final SelectorExpressionParser<GenericRecord, Object> PARSER = new SelectorExpressionParser.Builder<GenericRecord, Object>()
                .withFieldEvaluator(FieldGetter::new)
                .withArrayEvaluator(ArrayGetter::new)
                .build();

        public GenericRecordBaseSelector(String name, String expectedRoot, String expression) {
            super(name, expression);
            this.linkedNode = PARSER.parse(expectedRoot, expression);
        }

        protected Value eval(GenericRecord record) {
            Object value = record;
            GenericRecord currentRecord = record;
            LinkedNode<NodeEvaluator<GenericRecord, Object>> currentLinkedNode = linkedNode;
            while (currentLinkedNode != null) {
                if (value instanceof GenericRecord genericRecord) {
                    currentRecord = genericRecord;
                    NodeEvaluator<GenericRecord, Object> evaluator = currentLinkedNode.value();
                    value = evaluator.get(currentRecord);
                    currentLinkedNode = currentLinkedNode.next();
                    continue;
                }
                ValueException.throwConversionError("GenericRecord");
            }

            return Value.of(name(), value.toString());
        }
    }

    static class GenericRecordKeySelectorSupplier implements KeySelectorSupplier<GenericRecord> {

        private final GenericRecordDeserializer deserializer;

        public GenericRecordKeySelectorSupplier(ConnectorConfig config) {
            this.deserializer = new GenericRecordDeserializer(config, true);
        }

        @Override
        public KeySelector<GenericRecord> newSelector(String name, String expression) {
            return new GenericRecordKeySelector(name, expectedRoot(), expression);
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
            return deserializer;
        }
    }

    static final class GenericRecordKeySelector extends GenericRecordBaseSelector
            implements KeySelector<GenericRecord> {

        GenericRecordKeySelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(ConsumerRecord<GenericRecord, ?> record) {
            return super.eval(record.key());
        }
    }

    static class GenericRecordValueSelectorSupplier implements ValueSelectorSupplier<GenericRecord> {

        private final GenericRecordDeserializer deseralizer;

        GenericRecordValueSelectorSupplier(ConnectorConfig config) {
            this.deseralizer = new GenericRecordDeserializer(config, false);
        }

        @Override
        public ValueSelector<GenericRecord> newSelector(String name, String expression) {
            return new GenericRecordValueSelector(name, expectedRoot(), expression);
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
            return deseralizer;
        }
    }

    static final class GenericRecordValueSelector extends GenericRecordBaseSelector
            implements ValueSelector<GenericRecord> {

        public GenericRecordValueSelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(ConsumerRecord<?, GenericRecord> record) {
            return super.eval(record.value());
        }
    }

}
