package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExpressionParser<K, V> {

    public static class ParseException extends RuntimeException {

        ParseException(String message) {
            super(message);
        }
    }

    private static Pattern indexesPattern = Pattern.compile("\\[(\\d*)\\]");

    public interface NodeEvaluator<K, V> {

        V get(K k);
    }

    public interface FieldEvaluator<K, V> extends NodeEvaluator<K, V> {
    }

    public static class Builder<K, V> {

        private Function<String, NodeEvaluator<K, V>> fe;

        private BiFunction<String, List<Integer>, NodeEvaluator<K, V>> ae;

        public Builder<K, V> withFieldEvaluator(Function<String, NodeEvaluator<K, V>> fe) {
            this.fe = fe;
            return this;
        }

        public Builder<K, V> withArrayEvaluator(
                BiFunction<String, List<Integer>, NodeEvaluator<K, V>> ae) {
            this.ae = ae;
            return this;
        }

        public ExpressionParser<K, V> build() {
            return new ExpressionParser<>(fe, ae);
        }
    }

    private Function<String, NodeEvaluator<K, V>> fieldEvaluatorFactory;

    private BiFunction<String, List<Integer>, NodeEvaluator<K, V>> arrayEvaluatorFactory;

    ExpressionParser(Function<String, NodeEvaluator<K, V>> fe,
            BiFunction<String, List<Integer>, NodeEvaluator<K, V>> ae) {
        this.fieldEvaluatorFactory = fe;
        this.arrayEvaluatorFactory = ae;
    }

    public List<NodeEvaluator<K, V>> parse(String expression) {
        StringTokenizer st = new StringTokenizer(expression, ".");
        parseRoot(st);
        return parseTokens(st);
    }

    private void parseRoot(StringTokenizer st) {
        if (!st.hasMoreTokens()) {
            throw new ParseException("Expected root token");
        }
    }

    private List<NodeEvaluator<K, V>> parseTokens(StringTokenizer st) {
        List<NodeEvaluator<K, V>> evaluators = new ArrayList<>();
        while (st.hasMoreTokens()) {
            String fieldName = st.nextToken();
            int lbracket = fieldName.indexOf('[');
            if (lbracket != -1) {
                List<Integer> parseIndexes = parseIndexes(fieldName.substring(lbracket));
                String field = fieldName.substring(0, lbracket);
                evaluators.add(arrayEvaluatorFactory.apply(field, parseIndexes));
            } else {
                evaluators.add(fieldEvaluatorFactory.apply(fieldName));
            }
        }
        return evaluators;
    }

    private static List<Integer> parseIndexes(String indexedExpression) {
        List<Integer> indexes = new ArrayList<>();
        Matcher matcher = indexesPattern.matcher(indexedExpression);
        int previousEnd = 0;
        while (matcher.find()) {
            int currentStart = matcher.start();
            if (currentStart != previousEnd) {
                String invalidTerm = indexedExpression.substring(previousEnd, currentStart);
                throw new RuntimeException("No valid expression: " + invalidTerm);
            }
            previousEnd = matcher.end();
            String group = matcher.group(1);
            if (group.length() > 0) {
                indexes.add(Integer.parseInt(matcher.group(1)));
            } else {
                indexes.add(-1); // Splat expression
            }
        }
        return indexes;
    }

}
