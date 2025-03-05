
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

package com.lightstreamer.kafka.common.mapping.selectors;

import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.utils.Either;

import java.util.ArrayList;
import java.util.List;
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

        String asText();
    }

    public interface NodeEvaluator<T extends Node<T>> {
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

        String name();

        Node<T> eval(Node<T> node) throws ValueException;
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

    public static final class ParsingContext {
        private final String name;
        private final ExtractionExpression expression;
        private final Constant expectedRoot;
        private final String[] tokens;

        private int tokenIndex = 0;

        ParsingContext(String name, ExtractionExpression expression, Constant expectedRoot) {
            this.name = name;
            this.expression = expression;
            this.expectedRoot = expectedRoot;
            this.tokens = expression.tokens();
        }

        String name() {
            return name;
        }

        String expression() {
            return expression.expression();
        }

        Constant expectedRoot() {
            return expectedRoot;
        }

        void matchRoot() throws ExtractionException {
            if (!expectedRoot.equals(expression.constant())) {
                throw ExtractionException.expectedRootToken(name, expectedRoot.toString());
            }
            next();
        }

        boolean hasNext() {
            return tokenIndex < tokens.length;
        }

        String next() {
            String currentToken = tokens[tokenIndex];
            tokenIndex += 2;
            return currentToken;
        }
    }

    public static class SelectorExpressionParser<T extends Node<T>> {

        private static Pattern INDEXES = Pattern.compile("\\[(?:'([^']*)'|(\\d+))\\]");

        private static List<GeneralizedKey> parseIndexes(
                ParsingContext ctx, String indexedExpression) throws ExtractionException {
            List<GeneralizedKey> indexes = new ArrayList<>();
            Matcher matcher = INDEXES.matcher(indexedExpression);
            int previousEnd = 0;
            while (matcher.find()) {
                int currentStart = matcher.start();
                if (currentStart != previousEnd) {
                    throw ExtractionException.invalidIndexedExpression(
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
                throw ExtractionException.invalidIndexedExpression(ctx.name(), ctx.expression());
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

        public LinkedNodeEvaluator<T> parse(ParsingContext ctx) throws ExtractionException {
            LinkedNodeEvaluator<T> root = parseRoot(ctx);
            return parseTokens(root, ctx);
        }

        private LinkedNodeEvaluator<T> parseRoot(ParsingContext ctx) throws ExtractionException {
            ctx.matchRoot();
            return new LinkedNodeEvaluator<T>(NodeEvaluator.identity("root"));
        }

        private LinkedNodeEvaluator<T> parseTokens(LinkedNodeEvaluator<T> head, ParsingContext ctx)
                throws ExtractionException {
            LinkedNodeEvaluator<T> current = head;
            while (ctx.hasNext()) {
                String token = ctx.next();
                if (token.isBlank()) {
                    throw ExtractionException.missingToken(ctx.name(), ctx.expression());
                }
                int lbracket = token.indexOf('[');
                NodeEvaluator<T> node;
                if (lbracket != -1) {
                    String indexedExpression = token.substring(lbracket);
                    List<GeneralizedKey> indexes = parseIndexes(ctx, indexedExpression);
                    if (indexes.isEmpty()) {
                        throw ExtractionException.invalidIndexedExpression(
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
}
