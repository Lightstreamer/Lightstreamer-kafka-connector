package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;

public class SelectorExpressionParser<K, V> {

    public static String SELECTION_REGEX = "\\$\\{(.*)\\}";

    private static Pattern SELECTOR_PATTERN = Pattern.compile(SELECTION_REGEX);

    private static Pattern INDEXES = Pattern.compile("\\[(\\d*)\\]");

    public interface NodeEvaluator<K, V> {
        V get(K k);
    }

    public static class LinkedNode<T> {

        private LinkedNode<T> next;

        private final T value;

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

        public SelectorExpressionParser<K, V> build() {
            return new SelectorExpressionParser<>(fe, ae);
        }
    }

    private Function<String, NodeEvaluator<K, V>> fieldEvaluatorFactory;

    private BiFunction<String, List<Integer>, NodeEvaluator<K, V>> arrayEvaluatorFactory;

    SelectorExpressionParser(Function<String, NodeEvaluator<K, V>> fe,
            BiFunction<String, List<Integer>, NodeEvaluator<K, V>> ae) {
        this.fieldEvaluatorFactory = fe;
        this.arrayEvaluatorFactory = ae;
    }

    public LinkedNode<NodeEvaluator<K, V>> parse(String expectedRoot, String expression) {
        // Matcher matcher = SELECTOR_PATTERN.matcher(expression);
        // if (!matcher.matches()) {
        // throw new ParseException("Invalid selector expression:
        // \"%s\"".formatted(expression));
        // }
        // try (Scanner scanner = new Scanner(matcher.group(1)).useDelimiter("\\.")) {
        // parseRoot(scanner, expectedRoot);
        // return parseTokens(scanner);
        // }
        try (Scanner scanner = new Scanner(expression).useDelimiter("\\.")) {
            parseRoot(scanner, expectedRoot);
            return parseTokens(scanner);
        }
    }

    private void parseRoot(Scanner scanner, String expectedRoot) {
        if (!scanner.hasNext()) {
            throw new ExpressionException("Expected root token");
        }
        if (!expectedRoot.equals(scanner.next())) {
            ExpressionException.throwExpectedToken(expectedRoot);
        }
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
            ExpressionException.throwInvalidExpression();
        }
        return head;
    }

    private static List<Integer> parseIndexes(String indexedExpression) {
        List<Integer> indexes = new ArrayList<>();
        Matcher matcher = INDEXES.matcher(indexedExpression);
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
