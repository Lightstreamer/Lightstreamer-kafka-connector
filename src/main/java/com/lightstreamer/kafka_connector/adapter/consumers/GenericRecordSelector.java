package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.BaseValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

interface ExprEvaluator {
    Object get(GenericRecord record);
}

class RootGetter implements ExprEvaluator {

    private String name;

    public RootGetter(String name) {
        this.name = name;
    }

    @Override
    public Object get(GenericRecord record) {
        String recordName = record.getSchema().getName();
        if (!recordName.equals(name)) {
            throw new RuntimeException("No root  <" + name + "> exists! Found <>" + recordName + ">");
        }
        return record;
    }
}

class FieldGetter implements ExprEvaluator {

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

class IndexedFieldGetter implements ExprEvaluator {

    private static Pattern pattern = Pattern.compile("\\[(\\d+)\\]");

    private String name;

    private final FieldGetter getter;

    private final List<Integer> indexes;

    public IndexedFieldGetter(String fieldName, int lbracket) {
        this.name = fieldName.substring(0, lbracket);
        this.getter = new FieldGetter(name);
        this.indexes = parse(fieldName.substring(lbracket));
    }

    private static List<Integer> parse(String indexedExpression) {
        List<Integer> index = new ArrayList<>();
        Matcher matcher = pattern.matcher(indexedExpression);
        int previousEnd = 0;
        while (matcher.find()) {
            int currentStart = matcher.start();
            if (currentStart != previousEnd) {
                String invalidTerm = indexedExpression.substring(previousEnd, currentStart);
                throw new RuntimeException("No valid expression: " + invalidTerm);
            }
            previousEnd = matcher.end();
            index.add(Integer.parseInt(matcher.group(1)));
        }
        return index;
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

public class GenericRecordSelector extends BaseValueSelector<String, GenericRecord> {

    private List<ExprEvaluator> evaluatorsList;

    public GenericRecordSelector(String name, String expression) {
        super(name, expression);
        System.out.println("Creating navigator for " + expression);
        Objects.requireNonNull(expression);
        this.evaluatorsList = evaluators(expression);
    }

    List<ExprEvaluator> evaluators(String stmt) {
        List<ExprEvaluator> fieldEvaluators = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(stmt, ".");
        if (st.hasMoreTokens()) {
            fieldEvaluators.add(new RootGetter(st.nextToken()));
        }

        while (st.hasMoreTokens()) {
            String fieldName = st.nextToken();
            int lbracket = fieldName.indexOf('[');
            if (lbracket != -1) {
                fieldEvaluators.add(new IndexedFieldGetter(fieldName, lbracket));
            } else {
                fieldEvaluators.add(new FieldGetter(fieldName));
            }
        }
        return fieldEvaluators;
    }

    @Override
    public Value extract(ConsumerRecord<String, GenericRecord> record) {
        Object value = null;
        GenericRecord currentRecord = record.value();
        Iterator<ExprEvaluator> iterator = evaluatorsList.iterator();

        while (iterator.hasNext()) {
            ExprEvaluator evaluator = iterator.next();
            value = evaluator.get(currentRecord);
            if (iterator.hasNext()) {
                currentRecord = (GenericRecord) value;
            }
        }
        return new SimpleValue(name(), value.toString());
    }
}
