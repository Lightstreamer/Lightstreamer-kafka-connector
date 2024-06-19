
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.mapping.selectors;

import com.lightstreamer.kafka.utils.Either;

import org.apache.avro.util.Utf8;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectorExpressionParser<K, V> {

    public static String SELECTION_REGEX = "\\#\\{(.+)\\}";

    private static Pattern INDEXES = Pattern.compile("\\[(?:'([^']*)'|(\\d+))\\]");

    public interface NodeEvaluator<K, V> {
        String name();

        V get(K k) throws ValueException;
    }

    public static class LinkedNode<T> {

        private LinkedNode<T> next;

        private LinkedNode<T> prev;

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

        public LinkedNode<T> previous() {
            return prev;
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

        public Builder<K, V> withFieldEvaluator(
                Function<String, NodeEvaluator<K, V>> fieldEvaluator) {
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

    record ParsingContext(String name, String expression, String expectedRoot) {}

    private Function<String, NodeEvaluator<K, V>> fieldEvaluatorFactory;

    private BiFunction<String, List<GeneralizedKey>, NodeEvaluator<K, V>> arrayEvaluatorFactory;

    SelectorExpressionParser(Builder<K, V> builder) {
        this.fieldEvaluatorFactory = builder.fieldEvaluator;
        this.arrayEvaluatorFactory = builder.arrayEvaluator;
    }

    public LinkedNode<NodeEvaluator<K, V>> parse(
            String name, String expression, String expectedRoot) throws ExpressionException {
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
            String token = scanner.next();
            if (token.isBlank()) {
                ExpressionException.throwBlankToken(ctx.name(), ctx.expression());
            }
            int lbracket = token.indexOf('[');
            NodeEvaluator<K, V> node;
            if (lbracket != -1) {
                String indexedExpression = token.substring(lbracket);
                List<GeneralizedKey> indexes = parseIndexes(ctx, indexedExpression);
                if (indexes.isEmpty()) {
                    ExpressionException.throwInvalidIndexedExpression(ctx.name(), ctx.expression());
                }
                String field = token.substring(0, lbracket);
                node = arrayEvaluatorFactory.apply(field, indexes);
            } else {
                node = fieldEvaluatorFactory.apply(token);
            }
            LinkedNode<NodeEvaluator<K, V>> linkedNode = new LinkedNode<>(node);
            if (current == null) {
                current = linkedNode;
                head = linkedNode;
            } else {
                current.next = linkedNode;
                linkedNode.prev = current;
                current = linkedNode;
            }
        }
        if (head == null) {
            ExpressionException.throwInvalidExpression(ctx.name(), ctx.expression());
        }
        return head;
    }

    private static List<GeneralizedKey> parseIndexes(ParsingContext ctx, String indexedExpression) {
        List<GeneralizedKey> indexes = new ArrayList<>();
        Matcher matcher = INDEXES.matcher(indexedExpression);
        int previousEnd = 0;
        while (matcher.find()) {
            int currentStart = matcher.start();
            if (currentStart != previousEnd) {
                ExpressionException.throwInvalidIndexedExpression(ctx.name(), ctx.expression());
            }
            previousEnd = matcher.end();
            String key = matcher.group(1);
            String index = matcher.group(2);
            GeneralizedKey gk =
                    key != null
                            ? GeneralizedKey.key(key)
                            : GeneralizedKey.index(Integer.valueOf(index));
            indexes.add(gk);
        }
        if (previousEnd < indexedExpression.length()) {
            ExpressionException.throwInvalidIndexedExpression(ctx.name(), ctx.expression());
        }
        return indexes;
    }
}
