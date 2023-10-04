package com.lightstreamer.kafka_connector.adapter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

interface FieldEvaluator {
    Object get(GenericRecord record);
}

class FieldGetter implements FieldEvaluator {

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

class IndexedFieldGetter implements FieldEvaluator {

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
        System.out.println("Got value " + value + " <" + value.getClass().getName() + ">");
        System.out.println("Getting indexed value for " + indexes + " indexed");
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

public class RecordNavigator {

    private final String statement;

    private String mappedField;

    private List<FieldEvaluator> evaluatorsList;

    RecordNavigator(String mappedField, String statement) {
        System.out.println("Creating navigator for " + statement);
        Objects.requireNonNull(mappedField);
        Objects.requireNonNull(statement);
        this.mappedField = mappedField;
        this.statement = statement;
        this.evaluatorsList = evaluators(statement);
    }

    public String getMappedField() {
        return mappedField;
    }

    // private Object getField(GenericRecord record, String fieldName) {
    // if (!record.hasField(fieldName)) {
    // throw new RuntimeException("No field <" + fieldName + "> exists!");
    // }
    // return record.get(fieldName);
    // }

    // private Object getIndexedField(GenericRecord record, String fieldName, int
    // lbracket) {
    // String indexedFieldName = fieldName.substring(0, lbracket);
    // Object value = getField(record, indexedFieldName);
    // List<Integer> indexes = parse(fieldName.substring(lbracket));
    // for (int i : indexes) {
    // GenericData.Array<?> array = (GenericData.Array<?>) value;
    // value = array.get(i);
    // }
    // return value;
    // }

    List<FieldEvaluator> evaluators(String stmt) {
        List<FieldEvaluator> fieldEvaluators = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(stmt, ".");
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

    String extract(GenericRecord record) {
        Object value = null;
        GenericRecord currentRecord = record;
        Iterator<FieldEvaluator> iterator = evaluatorsList.iterator();

        while (iterator.hasNext()) {
            FieldEvaluator evaluator = iterator.next();
            value = evaluator.get(currentRecord);
            if (iterator.hasNext()) {
                currentRecord = (GenericRecord) value;
            }
        }
        return value.toString();
    }

    // String extract(GenericRecord record) {
    // GenericRecord currentRecord = record;
    // StringTokenizer st = new StringTokenizer(statement, ".");
    // Object value = null;
    // while (st.hasMoreTokens()) {
    // String fieldName = st.nextToken();
    // int lbracket = fieldName.indexOf('[');
    // if (lbracket != -1) {
    // value = getIndexedField(currentRecord, fieldName, lbracket);
    // } else {
    // value = getField(currentRecord, fieldName);
    // }

    // if (st.hasMoreTokens()) {
    // currentRecord = (GenericRecord) value;
    // }
    // }
    // return value.toString();
    // }
}
