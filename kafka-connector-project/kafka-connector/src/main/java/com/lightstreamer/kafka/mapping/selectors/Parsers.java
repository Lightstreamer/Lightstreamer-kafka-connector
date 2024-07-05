
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

import com.lightstreamer.kafka.mapping.selectors.SelectorSupplier.Constant;
import com.lightstreamer.kafka.utils.Either;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface Parsers {

    public interface Node<T extends Node<T>> {

        boolean has(String propertyname);

        Node<T> get(String propertyName);

        boolean isArray();

        int size();

        Node<T> get(int index);

        boolean isNull();

        boolean isScalar();

        String asText(String defaultStr);
    }

    public static class SimpleNode implements Node<SimpleNode> {

        private final Object value;

        public SimpleNode(Object value) {
            this.value = value;
        }

        @Override
        public boolean has(String propertyname) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'has'");
        }

        @Override
        public Node<SimpleNode> get(String propertyName) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'get'");
        }

        @Override
        public boolean isArray() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'isArray'");
        }

        @Override
        public int size() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'size'");
        }

        @Override
        public Node<SimpleNode> get(int index) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'get'");
        }

        @Override
        public boolean isNull() {
            return this.value == null;
        }

        @Override
        public boolean isScalar() {
            return true;
        }

        @Override
        public String asText(String defaultStr) {
            return Objects.toString(value, defaultStr);
        }
    }

    public interface NodeEvaluator<T extends Node<T>> {
        String name();

        Node<T> eval(Node<T> node) throws ValueException;

        static <T extends Node<T>> NodeEvaluator<T> identity(String name) {
            return new NodeEvaluator<T>() {

                @Override
                public String name() {
                    return name;
                }

                @Override
                public Node<T> eval(Node<T> node) throws ValueException {
                    return node;
                }
            };
        }
    }

    public static final class LinkedNodeEvaluator<T extends Node<T>> {

        private LinkedNodeEvaluator<T> next;
        private LinkedNodeEvaluator<T> prev;
        private final NodeEvaluator<T> current;

        LinkedNodeEvaluator(NodeEvaluator<T> evaluator) {
            this.current = evaluator;
        }

        public boolean hasNext() {
            return next != null;
        }

        public NodeEvaluator<T> current() {
            return current;
        }

        public LinkedNodeEvaluator<T> next() {
            return next;
        }

        public LinkedNodeEvaluator<T> previous() {
            return prev;
        }
    }

    public static class GeneralizedKey {

        static GeneralizedKey index(int index) {
            return new GeneralizedKey(index);
        }

        static GeneralizedKey key(String key) {
            return new GeneralizedKey(key);
        }

        private Either<String, Integer> genericKey;

        GeneralizedKey(String key) {
            genericKey = Either.left(key);
        }

        GeneralizedKey(int index) {
            genericKey = Either.right(index);
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

    static final class ParsingContext {

        private final String name;
        private final String expression;
        private final Constant expectedRoot;

        private final String[] tokens;
        private int tokenIndex = 0;

        ParsingContext(String name, String expression, Constant expectedRoot) {
            this.name = name;
            this.expression = expression;
            this.expectedRoot = expectedRoot;
            tokens = expression.splitWithDelimiters("\\.", 0);
        }

        String name() {
            return name;
        }

        String expression() {
            return expression;
        }

        Constant expectedRoot() {
            return expectedRoot;
        }

        void matchRoot() {
            if (!(hasNext() && next().equals(expectedRoot.toString()))) {
                throw ExpressionException.expectedRootToken(name, expectedRoot.toString());
            }
        }

        boolean hasNext() {
            return tokenIndex < tokens.length;
        }

        boolean hasTrailingDelimiters() {
            return tokens[tokens.length - 1].equals(".");
        }

        String next() {
            String currentToken = tokens[tokenIndex];
            tokenIndex += 2;
            return currentToken;
        }
    }

    public class SelectorExpressionParser<T extends Node<T>> {

        private static Pattern INDEXES = Pattern.compile("\\[(?:'([^']*)'|(\\d+))\\]");

        private static List<GeneralizedKey> parseIndexes(
                ParsingContext ctx, String indexedExpression) {
            List<GeneralizedKey> indexes = new ArrayList<>();
            Matcher matcher = INDEXES.matcher(indexedExpression);
            int previousEnd = 0;
            while (matcher.find()) {
                int currentStart = matcher.start();
                if (currentStart != previousEnd) {
                    throw ExpressionException.invalidIndexedExpression(
                            ctx.name(), ctx.expression());
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
                throw ExpressionException.invalidIndexedExpression(ctx.name(), ctx.expression());
            }
            return indexes;
        }

        private Function<String, NodeEvaluator<T>> fieldEvaluatorFactory;
        private BiFunction<String, List<GeneralizedKey>, NodeEvaluator<T>> arrayEvaluatorFactory;

        public SelectorExpressionParser(
                Function<String, NodeEvaluator<T>> fieldEvaluator,
                BiFunction<String, List<GeneralizedKey>, NodeEvaluator<T>> arrayEvaluator) {
            this.fieldEvaluatorFactory = fieldEvaluator;
            this.arrayEvaluatorFactory = arrayEvaluator;
        }

        public LinkedNodeEvaluator<T> parse(String name, String expression, Constant expectedRoot)
                throws ExpressionException {
            ParsingContext ctx = new ParsingContext(name, expression, expectedRoot);
            if (ctx.hasTrailingDelimiters()) {
                throw ExpressionException.unexpectedTrailingDots(name, expression);
            }
            LinkedNodeEvaluator<T> root = parseRoot(ctx);
            return parseTokens(root, ctx);
        }

        private LinkedNodeEvaluator<T> parseRoot(ParsingContext ctx) {
            ctx.matchRoot();
            return new LinkedNodeEvaluator<T>(NodeEvaluator.identity("root"));
        }

        private LinkedNodeEvaluator<T> parseTokens(
                LinkedNodeEvaluator<T> head, ParsingContext ctx) {
            LinkedNodeEvaluator<T> current = head;
            while (ctx.hasNext()) {
                String token = ctx.next();
                if (token.isBlank()) {
                    throw ExpressionException.missingToken(ctx.name(), ctx.expression());
                }
                int lbracket = token.indexOf('[');
                NodeEvaluator<T> node;
                if (lbracket != -1) {
                    String indexedExpression = token.substring(lbracket);
                    List<GeneralizedKey> indexes = parseIndexes(ctx, indexedExpression);
                    if (indexes.isEmpty()) {
                        throw ExpressionException.invalidIndexedExpression(
                                ctx.name(), ctx.expression());
                    }
                    String field = token.substring(0, lbracket);
                    node = arrayEvaluatorFactory.apply(field, indexes);
                } else {
                    node = fieldEvaluatorFactory.apply(token);
                }
                LinkedNodeEvaluator<T> linkedNode = new LinkedNodeEvaluator<>(node);
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
    }

    public static final String SELECTION_REGEX = "\\#\\{(.+)\\}";
}
