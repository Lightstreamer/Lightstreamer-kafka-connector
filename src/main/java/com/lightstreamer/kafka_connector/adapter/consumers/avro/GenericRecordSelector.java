package com.lightstreamer.kafka_connector.adapter.consumers.avro;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser;
import com.lightstreamer.kafka_connector.adapter.evaluator.ExpressionParser.NodeEvaluator;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

interface ExprEvaluator {
    Object get(GenericRecord record);
}

// class RootGetter implements ExprEvaluator {

// private String name;

// public RootGetter(String name) {
// this.name = name;
// }

// @Override
// public Object get(GenericRecord record) {
// String recordName = record.getSchema().getName();
// if (!recordName.equals(name)) {
// throw new RuntimeException("No root <" + name + "> exists! Found <>" +
// recordName + ">");
// }
// return record;
// }
// }

class FieldGetter implements NodeEvaluator<GenericRecord, Object> {

    private String name;

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

class ArrayGetter implements NodeEvaluator<GenericRecord, Object> {

    private final String name;

    private final FieldGetter getter;

    private final List<Integer> indexes;

    public ArrayGetter(String fieldName, List<Integer> indexes) {
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

public class GenericRecordSelector extends BaseSelector<GenericRecord> {

    private List<NodeEvaluator<GenericRecord, Object>> evaluatorsList;

    private static final ExpressionParser<GenericRecord, Object> PARSER = new ExpressionParser.Builder<GenericRecord, Object>()
            .withFieldEvaluator(FieldGetter::new)
            .withArrayEvaluator(ArrayGetter::new)
            .build();

    public GenericRecordSelector(String name, String expression) {
        super(name, expression);
        Objects.requireNonNull(expression);
        this.evaluatorsList = PARSER.parse(expression);
    }

    // List<ExprEvaluator> evaluators(String expression) {
    // List<ExprEvaluator> fieldEvaluators = new ArrayList<>();
    // StringTokenizer st = new StringTokenizer(expression, ".");
    // if (st.hasMoreTokens()) {
    // fieldEvaluators.add(new RootGetter(st.nextToken()));
    // }

    // while (st.hasMoreTokens()) {
    // String fieldName = st.nextToken();
    // int lbracket = fieldName.indexOf('[');
    // if (lbracket != -1) {
    // fieldEvaluators.add(new IndexedFieldGetter(fieldName, lbracket));
    // } else {
    // fieldEvaluators.add(new FieldGetter(fieldName));
    // }
    // }
    // return fieldEvaluators;
    // }

    @Override
    public Value extract(GenericRecord record) {
        Object value = null;
        GenericRecord currentRecord = record;
        Iterator<NodeEvaluator<GenericRecord, Object>> iterator = evaluatorsList.iterator();

        while (iterator.hasNext()) {
            NodeEvaluator<GenericRecord, Object> evaluator = iterator.next();
            value = evaluator.get(currentRecord);
            if (iterator.hasNext()) {
                currentRecord = (GenericRecord) value;
            }
        }
        return new SimpleValue(name(), value.toString());
    }
}
