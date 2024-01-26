package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;

import com.lightstreamer.kafka_connector.adapter.commons.Either;
import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;

public class SelectorExpressionParser<K, V> {

    public static String SELECTION_REGEX = "\\#\\{(.*)\\}";

    private static Pattern SELECTOR_PATTERN = Pattern.compile(SELECTION_REGEX);

    // private static Pattern INDEXES = Pattern.compile("\\[(\\d+)\\]$");

    private static Pattern KEYS = Pattern.compile("\\['(.*)'\\]$");

    private static Pattern INDEXES = Pattern.compile("\\[(?:'([^']*)'|(\\d+))\\]");

    // Pattern.compile("\\['([^']*)'\\]");

    public interface NodeEvaluator<K, V> {
        V get(K k) throws ValueException;
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

    public static class GeneralizedKey {

        private Either<Utf8, Integer> genericKey;

        GeneralizedKey(Utf8 key) {
            genericKey = Either.left(key);
        }

        GeneralizedKey(int index) {
            genericKey = Either.right(index);
        }

        static GeneralizedKey index(int index) {
            return new GeneralizedKey(index);
        }

        static GeneralizedKey key(String key) {
            return new GeneralizedKey(new Utf8(key));
        }

        public boolean isKey() {
            return genericKey.isLeft();
        }

        public Utf8 key() {
            return genericKey.getLeft();
        }

        public boolean isIndex() {
            return genericKey.isRight();
        }

        public int index() {
            return genericKey.getRight();
        }

        public String toString() {
            return isKey() ? "Key = [%s]".formatted(key()) : "Index = [%d]".formatted(index());
        }
    }

    public static class Builder<K, V> {

        private Function<String, NodeEvaluator<K, V>> fieldEvaluator;

        private BiFunction<String, List<GeneralizedKey>, NodeEvaluator<K, V>> arrayEvaluator;

        public Builder<K, V> withFieldEvaluator(Function<String, NodeEvaluator<K, V>> fieldEvaluator) {
            this.fieldEvaluator = fieldEvaluator;
            return this;
        }

        public Builder<K, V> withGenericIndexedEvaluator(
                BiFunction<String, List<GeneralizedKey>, NodeEvaluator<K, V>> arrayEvaluator) {
            this.arrayEvaluator = arrayEvaluator;
            return this;
        }

        public SelectorExpressionParser<K, V> build() {
            return new SelectorExpressionParser<>(this);
        }
    }

    record ParsingContext(String name, String expression, String expectedRoot) {

    }

    private Function<String, NodeEvaluator<K, V>> fieldEvaluatorFactory;

    private BiFunction<String, List<GeneralizedKey>, NodeEvaluator<K, V>> arrayEvaluatorFactory;

    SelectorExpressionParser(Builder<K, V> builder) {
        this.fieldEvaluatorFactory = builder.fieldEvaluator;
        this.arrayEvaluatorFactory = builder.arrayEvaluator;
    }

    public LinkedNode<NodeEvaluator<K, V>> parse(String name, String expression, String expectedRoot)
            throws ExpressionException {
        ParsingContext ctx = new ParsingContext(name, expression, expectedRoot);
        try (Scanner scanner = new Scanner(expression).useDelimiter("\\.")) {
            parseRoot(scanner, ctx);
            return parseTokens(scanner, ctx);
        }
    }

    private void parseRoot(Scanner scanner, ParsingContext ctx) {
        if (!scanner.hasNext()) {
            ExpressionException.throwExpectedRootToken(ctx.name(), ctx.expectedRoot());
        }
        if (!ctx.expectedRoot().equals(scanner.next())) {
            ExpressionException.throwExpectedRootToken(ctx.name(), ctx.expectedRoot());
        }
    }

    private LinkedNode<NodeEvaluator<K, V>> parseTokens(Scanner scanner, ParsingContext ctx) {
        LinkedNode<NodeEvaluator<K, V>> head = null, current = null;
        while (scanner.hasNext()) {
            String fieldName = scanner.next();
            if (fieldName.isBlank()) {
                ExpressionException.throwBlankToken(ctx.name(), ctx.expression());
            }
            int lbracket = fieldName.indexOf('[');
            NodeEvaluator<K, V> node;
            if (lbracket != -1) {
                List<GeneralizedKey> indexes = parseIndexes(fieldName.substring(lbracket));
                String field = fieldName.substring(0, lbracket);
                if (indexes.isEmpty()) {
                    ExpressionException.throwInvalidIndexedExpression(ctx.name(), ctx.expression());
                }
                node = arrayEvaluatorFactory.apply(field, indexes);
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
            ExpressionException.throwInvalidExpression(ctx.name(), ctx.expression());
        }
        return head;
    }

    private static List<GeneralizedKey> parseIndexes(String indexedExpression) {
        List<GeneralizedKey> indexes = new ArrayList<>();
        Matcher matcher = INDEXES.matcher(indexedExpression);
        int previousEnd = 0;
        while (matcher.find()) {
            int currentStart = matcher.start();
            if (currentStart != previousEnd) {
                String invalidTerm = indexedExpression.substring(previousEnd, currentStart);
                throw new RuntimeException("No valid expression: " + invalidTerm);
            }
            previousEnd = matcher.end();
            String key = matcher.group(1);
            String index = matcher.group(2);
            if (key != null) {
                indexes.add(GeneralizedKey.key(key));
            } else if (index != null) {
                indexes.add(GeneralizedKey.index(Integer.valueOf(index)));
            }

        }
        return indexes;
    }

}
