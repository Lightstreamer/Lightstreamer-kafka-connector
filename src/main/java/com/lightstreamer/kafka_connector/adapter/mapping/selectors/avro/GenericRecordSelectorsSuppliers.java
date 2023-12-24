package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class GenericRecordSelectorsSuppliers {

    private static final GenericRecordKeySelectorSupplier KEY_SELECTOR_SUPPLIER = new GenericRecordKeySelectorSupplier();

    private static final GenericRecordValueSelectorSupplier VALUE_SELECTOR_SUPPLIER = new GenericRecordValueSelectorSupplier();

    public static KeySelectorSupplier<GenericRecord> keySelectorSupplier() {
        return KEY_SELECTOR_SUPPLIER;
    }

    public static ValueSelectorSupplier<GenericRecord> valueSelectorSupplier() {
        return VALUE_SELECTOR_SUPPLIER;
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
                    throw new RuntimeException("No field <" + name + "> exists!");
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
                        throw new RuntimeException("Current evaluated field is not an Array");
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

        protected Value eval(String tag, GenericRecord record) {
            Object value = record;
            GenericRecord currentRecord = record;
            LinkedNode<NodeEvaluator<GenericRecord, Object>> currentLinkedNode = linkedNode;
            while (currentLinkedNode != null) {
                if (value instanceof GenericRecord genericRecord) {
                    currentRecord = genericRecord;
                } else {
                    throw new RuntimeException("Conversion error");
                }
                NodeEvaluator<GenericRecord, Object> evaluator = currentLinkedNode.value();
                value = evaluator.get(currentRecord);
                currentLinkedNode = currentLinkedNode.next();
            }

            return Value.of(tag, name(), value.toString());
        }
    }

    static class GenericRecordKeySelectorSupplier extends AbstractSelectorSupplier<GenericRecord>
            implements KeySelectorSupplier<GenericRecord> {

        @Override
        public void configKey(Map<String, String> conf, Properties props) {
            KeySelectorSupplier.super.configKey(conf, props);
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_KEY_TYPE_CONFIG, true);
        }

        @Override
        protected Class<?> getLocalSchemaDeserializer() {
            return GenericRecordLocalSchemaDeserializer.class;
        }

        @Override
        protected Class<?> getSchemaDeserializer() {
            return KafkaAvroDeserializer.class;
        }

        @Override
        public KeySelector<GenericRecord> newSelector(String name, String expression) {
            return new GenericRecordKeySelector(name, expectedRoot(), expression);
        }
    }

    static final class GenericRecordKeySelector extends GenericRecordBaseSelector
            implements KeySelector<GenericRecord> {

        GenericRecordKeySelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(String tag, ConsumerRecord<GenericRecord, ?> record) {
            return super.eval(tag, record.key());
        }
    }

    static class GenericRecordValueSelectorSupplier extends AbstractSelectorSupplier<GenericRecord>
            implements ValueSelectorSupplier<GenericRecord> {

        @Override
        protected Class<?> getLocalSchemaDeserializer() {
            return GenericRecordLocalSchemaDeserializer.class;
        }

        @Override
        protected Class<?> getSchemaDeserializer() {
            return KafkaAvroDeserializer.class;
        }

        @Override
        public String deserializer(Properties props) {
            return super.deserializer(false, props);
        }

        @Override
        public ValueSelector<GenericRecord> newSelector(String name, String expression) {
            return new GenericRecordValueSelector(name, expectedRoot(), expression);
        }
    }

    static final class GenericRecordValueSelector extends GenericRecordBaseSelector
            implements ValueSelector<GenericRecord> {

        public GenericRecordValueSelector(String name, String expectedRoot, String expression) {
            super(name, expectedRoot, expression);
        }

        @Override
        public Value extract(String tag, ConsumerRecord<?, GenericRecord> record) {
            return super.eval(tag, record.value());
        }
    }

}
