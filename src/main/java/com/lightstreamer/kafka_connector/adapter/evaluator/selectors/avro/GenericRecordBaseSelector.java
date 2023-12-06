package com.lightstreamer.kafka_connector.adapter.evaluator.selectors.avro;

import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SelectorExpressionParser.LinkedNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SelectorExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

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

    public GenericRecordBaseSelector(String name, String expression) {
        super(name, expression);
        this.linkedNode = PARSER.parse(expectedRoot(), expression);
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

        // } while (linkedNode.hasNext());
        // while (nodesList.hasNext()) {
        // NodeEvaluator<GenericRecord, Object> evaluator = nodesList.next();
        // value = evaluator.get(currentRecord);
        // if (iterator.hasNext()) {
        // currentRecord = (GenericRecord) value;
        // }
        // }
        return Value.of(name(), value.toString());
    }
}