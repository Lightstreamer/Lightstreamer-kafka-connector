package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser.NodeEvaluator;

sealed class GenericRecordBaseSelector extends BaseSelector
        permits GenericRecordKeySelectorSupplier.GenericRecordKeySelector,
        GenericRecordValueSelectorSupplier.GenericRecordValueSelector {

    private static class FieldGetter implements NodeEvaluator<GenericRecord, Object> {

        private final String name;

        public FieldGetter(String name) {
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
            this.name = fieldName;
            this.indexes = indexes;
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
            NodeEvaluator<GenericRecord, Object> evaluator = currentLinkedNode.value();
            currentRecord = (GenericRecord) value;
            value = evaluator.get(currentRecord);
            currentLinkedNode = currentLinkedNode.next();
        }

        return Value.of(name(), value.toString());
    }
}
