package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import com.lightstreamer.kafka_connector.adapter.commons.Either;
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
                return get(name, record);
            }

            public static Object get(String name, GenericRecord record) {
                if (!record.hasField(name)) {
                    ValueException.throwFieldNotFound(name);
                }
                return record.get(name);
            }

        }

        private static class ArrayGetter implements NodeEvaluator<GenericRecord, Object> {

            private final String name;

            private final FieldGetter getter;

            private final List<Either<String, Integer>> indexes;

            ArrayGetter(String fieldName, List<Either<String, Integer>> indexes) {
                this.name = Objects.requireNonNull(fieldName);
                this.indexes = Objects.requireNonNull(indexes);
                this.getter = new FieldGetter(name);
            }

            @Override
            public Object get(GenericRecord record) {
                Object value = getter.get(record);
                for (Either<String, Integer> i : indexes) {
                    if (i.isRight()) {
                        if (value instanceof GenericData.Array<?> array) {
                            try {
                                value = array.get(i.getRight());
                            } catch (IndexOutOfBoundsException ie) {
                                ValueException.throwIndexOfOutBoundex(i.getRight());
                            }
                        } else {
                            ValueException.throwNoIndexedField();
                        }
                    } else {
                        if (value instanceof Map map) {
                            value = map.get(i.getLeft());
                        } else if (value instanceof GenericRecord gr) {
                            value = FieldGetter.get(i.getLeft(), gr);
                        }
                        if (value == null) {
                            ValueException.throwNoKeyFound(i.getLeft());
                        }
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

        public GenericRecordBaseSelector(String name, String expression, String expectedRoot) {
            super(name, expression);
            this.linkedNode = PARSER.parse(name, expression, expectedRoot);
        }

        private boolean isScalar(Object value) {
            return !(value instanceof GenericRecord);
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

            if (!isScalar(value)) {
                ValueException.throwNonComplexObjectRequired(expression());
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
            return new GenericRecordKeySelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
            return deserializer;
        }
    }

    static final class GenericRecordKeySelector extends GenericRecordBaseSelector
            implements KeySelector<GenericRecord> {

        GenericRecordKeySelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
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
            return new GenericRecordValueSelector(name, expression, expectedRoot());
        }

        @Override
        public Deserializer<GenericRecord> deseralizer() {
            return deseralizer;
        }
    }

    static final class GenericRecordValueSelector extends GenericRecordBaseSelector
            implements ValueSelector<GenericRecord> {

        public GenericRecordValueSelector(String name, String expression, String expectedRoot) {
            super(name, expression, expectedRoot);
        }

        @Override
        public Value extract(ConsumerRecord<?, GenericRecord> record) {
            return super.eval(record.value());
        }
    }

}
