package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
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

    public static class LinkedNode<T> {

        private LinkedNode<T> next;

        private T value;

        LinkedNode(T value) {
            this.value = value;
        }

        public boolean hasNext() {
            return next != null;
        }

        public T value() {
            return value;
        }

        public LinkedNode<T> next() {
            return next;
        }
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

    public LinkedNode<NodeEvaluator<K, V>> parse(String expression) {
        Scanner scanner = new Scanner(expression).useDelimiter("\\.");
        parseRoot(scanner);
        return parseTokens(scanner);
    }

    private void parseRoot(Scanner scanner) {
        if (!scanner.hasNext()) {
            throw new ParseException("Expected root token");
        }
        scanner.next();
    }

    private LinkedNode<NodeEvaluator<K, V>> parseTokens(Scanner scanner) {
        LinkedNode<NodeEvaluator<K, V>> head = null, current = null;
        while (scanner.hasNext()) {
            String fieldName = scanner.next();
            int lbracket = fieldName.indexOf('[');
            NodeEvaluator<K, V> node;
            if (lbracket != -1) {
                List<Integer> parseIndexes = parseIndexes(fieldName.substring(lbracket));
                String field = fieldName.substring(0, lbracket);
                node = arrayEvaluatorFactory.apply(field, parseIndexes);
            } else {
                node = fieldEvaluatorFactory.apply(fieldName);
            }
            LinkedNode<NodeEvaluator<K, V>> linkedNode = new LinkedNode<>(node);
            if (current == null) {
                current = linkedNode;
                head = linkedNode;
            } else {
                current.next = linkedNode;
                current = linkedNode;
            }
        }
        if (head == null) {
            throw new ParseException("Invalid expression");
        }
        return head;
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
