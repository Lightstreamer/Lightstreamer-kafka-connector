
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

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectorExpressionParser<T> {

    public static String SELECTION_REGEX = "\\#\\{(.+)\\}";

    private static Pattern INDEXES = Pattern.compile("\\[(?:'([^']*)'|(\\d+))\\]");

    public interface NodeEvaluator<T> {
        String name();

        T eval(T t) throws ValueException;
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

        private Either<String, Integer> genericKey;

        GeneralizedKey(String key) {
            genericKey = Either.left(key);
        }

        GeneralizedKey(int index) {
            genericKey = Either.right(index);
        }

        static GeneralizedKey index(int index) {
            return new GeneralizedKey(index);
        }

        static GeneralizedKey key(String key) {
            return new GeneralizedKey(key);
        }

        public boolean isKey() {
            return genericKey.isLeft();
        }

        public String key() {
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

    public static class Builder<T> {

        private Function<String, NodeEvaluator<T>> fieldEvaluator;

        private BiFunction<String, List<GeneralizedKey>, NodeEvaluator<T>> arrayEvaluator;

        public Builder<T> withFieldEvaluator(Function<String, NodeEvaluator<T>> fieldEvaluator) {
            this.fieldEvaluator = fieldEvaluator;
            return this;
        }

        public Builder<T> withGenericIndexedEvaluator(
                BiFunction<String, List<GeneralizedKey>, NodeEvaluator<T>> arrayEvaluator) {
            this.arrayEvaluator = arrayEvaluator;
            return this;
        }

        public SelectorExpressionParser<T> build() {
            return new SelectorExpressionParser<>(this);
        }
    }

    record ParsingContext(String name, String expression, String expectedRoot) {}

    private Function<String, NodeEvaluator<T>> fieldEvaluatorFactory;

    private BiFunction<String, List<GeneralizedKey>, NodeEvaluator<T>> arrayEvaluatorFactory;

    SelectorExpressionParser(Builder<T> builder) {
        this.fieldEvaluatorFactory = builder.fieldEvaluator;
        this.arrayEvaluatorFactory = builder.arrayEvaluator;
    }

    public LinkedNode<NodeEvaluator<T>> parse(String name, String expression, String expectedRoot)
            throws ExpressionException {
        ParsingContext ctx = new ParsingContext(name, expression, expectedRoot);
        try (Scanner scanner = new Scanner(expression).useDelimiter("\\.")) {
            LinkedNode<NodeEvaluator<T>> root = parseRoot(scanner, ctx);
            return parseTokens(root, scanner, ctx);
        }
    }

    private LinkedNode<NodeEvaluator<T>> parseRoot(Scanner scanner, ParsingContext ctx) {
        if (!scanner.hasNext()) {
            ExpressionException.throwExpectedRootToken(ctx.name(), ctx.expectedRoot());
        }
        if (!ctx.expectedRoot().equals(scanner.next())) {
            ExpressionException.throwExpectedRootToken(ctx.name(), ctx.expectedRoot());
        }
        NodeEvaluator<T> rootEvaluator =
                new NodeEvaluator<T>() {

                    @Override
                    public String name() {
                        return "root";
                    }

                    @Override
                    public T eval(T t) throws ValueException {
                        return t;
                    }
                };
        return new LinkedNode<>(rootEvaluator);
    }

    private LinkedNode<NodeEvaluator<T>> parseTokens(
            LinkedNode<NodeEvaluator<T>> head, Scanner scanner, ParsingContext ctx) {
        LinkedNode<NodeEvaluator<T>> current = head;
        while (scanner.hasNext()) {
            String token = scanner.next();
            if (token.isBlank()) {
                ExpressionException.throwBlankToken(ctx.name(), ctx.expression());
            }
            int lbracket = token.indexOf('[');
            NodeEvaluator<T> node;
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
            LinkedNode<NodeEvaluator<T>> linkedNode = new LinkedNode<>(node);
            if (current == null) {
                current = linkedNode;
                head = linkedNode;
            } else {
                current.next = linkedNode;
                linkedNode.prev = current;
                current = linkedNode;
            }
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
