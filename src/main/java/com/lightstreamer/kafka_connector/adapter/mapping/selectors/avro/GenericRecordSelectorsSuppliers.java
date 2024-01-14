package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorSupplierConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

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

        protected Value eval(GenericRecord record) {
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

            return Value.of(name(), value.toString());
        }
    }

    static class GenericRecordKeySelectorSupplier implements KeySelectorSupplier<GenericRecord> {

        @Override
        public KeySelector<GenericRecord> newSelector(String name, String expression) {
            return new GenericRecordKeySelector(name, expectedRoot(), expression);
        }

        @Override
        public void config(Map<String, String> configuration, Properties props) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer(props));
        }

        @Override
        public String deserializer(Properties props) {
            if (props.get("key.schema.file") != null) {
                return GenericRecordLocalSchemaDeserializer.class.getName();
            }
            return KafkaAvroDeserializer.class.getName();
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

        private SelectorSupplierConfig config;

        GenericRecordValueSelectorSupplier() {
            config = new SelectorSupplierConfig(KafkaAvroDeserializer.class,
                    GenericRecordLocalSchemaDeserializer.class);
        }

        @Override
        public void config(Map<String, String> configuration, Properties props) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer(props));
            props.put("adapter.dir", configuration.get("adapter.dir"));
        }

        @Override
        public String deserializer(Properties props) {
            if (props.get("value.schema.file") != null) {
                return config.getLocalSchemaDeserializer().getName();
            }
            return config.getSchemaDeserializer().getName();
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
        public Value extract(ConsumerRecord<?, GenericRecord> record) {
            return super.eval(record.value());
        }
    }

}
