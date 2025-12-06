
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

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parsers {

    public interface Node<T extends Node<T>> extends Data {

        String name();

        default T getProperty(String nodeName, String propertyName) {
            if (isNull()) {
                throw ValueException.nullObject(propertyName);
            }

            if (isScalar()) {
                throw ValueException.scalarObject(propertyName);
            }

            if (!has(propertyName)) {
                throw ValueException.fieldNotFound(propertyName);
            }
            return get(nodeName, propertyName);
        }

        T get(String nodeName, String propertyName);

        default boolean isNull() {
            return false;
        }

        boolean isScalar();

        boolean has(String propertyname);

        default T getIndexed(String nodeName, int index, String indexedPropertyName) {
            if (isNull()) {
                throw ValueException.nullObject(index);
            }

            if (isArray()) {
                if (index < size()) {
                    return get(nodeName, index);
                } else {
                    throw ValueException.indexOfOutBounds(index);
                }
            }

            throw ValueException.noIndexedField(indexedPropertyName);
        }

        T get(String nodeName, int index);

        boolean isArray();

        int size();

        String text();

        default void flatIntoMap(Map<String, String> target) {}

        default void addToMap(Map<String, String> target) {
            target.put(name(), text());
        }

        static <P, T extends Node<T>> T createRoot(
                Supplier<P> payload, BiFunction<String, P, T> rootNode) {
            P object = payload.get();
            if (object == null) {
                return nullNode();
            }
            return (T) rootNode(propertyName -> rootNode.apply(propertyName, object));
        }

        @SuppressWarnings("unchecked")
        static <T extends Node<T>> T nullNode() {
            return (T) NullNode.INSTANCE;
        }

        static class NullNode<T extends Node<T>> implements Node<T> {

            private static final NullNode<?> INSTANCE = new NullNode<>();

            private NullNode() {
                // private constructor to prevent instantiation
            }

            @Override
            public String name() {
                return "NULL NODE";
            }

            @Override
            public boolean isNull() {
                return true;
            }

            @Override
            public boolean isScalar() {
                return true;
            }

            public boolean has(String propertyname) {
                return false;
            }

            @Override
            public T get(String nodeName, String propertyName) {
                return null;
            }

            @Override
            public boolean isArray() {
                return false;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public T get(String nodeName, int index) {
                return null;
            }

            @Override
            public String text() {
                return "null";
            }
        }

        static <T extends Node<T>> Node<T> rootNode(Function<String, T> rootNodeFactory) {
            return new RootNode<>(rootNodeFactory);
        }

        static class RootNode<T extends Node<T>> implements Node<T> {

            private final Function<String, T> rootNodeFactory;

            RootNode(Function<String, T> rootNodeFactory) {
                this.rootNodeFactory = Objects.requireNonNull(rootNodeFactory);
            }

            @Override
            public String name() {
                return "ROOT";
            }

            @Override
            public boolean isScalar() {
                return false;
            }

            @Override
            public boolean has(String propertyname) {
                return true;
            }

            @Override
            public T get(String nodeName, String propertyName) {
                return rootNodeFactory.apply(nodeName);
            }

            @Override
            public boolean isArray() {
                return false;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public T get(String nodeName, int index) {
                return null;
            }

            @Override
            public String text() {
                return "ROOT";
            }
        }
    }

    public static final class ParsingContext {

        private final ExtractionExpression expression;
        private final Constant expectedRoot;
        private final String[] tokens;

        private int tokenIndex = 0;

        ParsingContext(ExtractionExpression expression, Constant expectedRoot) {
            this.expression = expression;
            this.expectedRoot = expectedRoot;
            this.tokens = expression.tokens();
        }

        String expression() {
            return expression.expression();
        }

        Constant expectedRoot() {
            return expectedRoot;
        }

        void matchRoot() throws ExtractionException {
            if (!expectedRoot.equals(expression.constant())) {
                throw ExtractionException.expectedRootToken(
                        expression.expression(), expectedRoot.toString());
            }
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

    abstract static class NodeEvaluator<T extends Node<T>> {

        final String propertyName;
        private NodeEvaluator<T> next;

        NodeEvaluator(String propertyName) {
            this.propertyName = Objects.requireNonNull(propertyName);
        }

        final String propertyName() {
            return propertyName;
        }

        final boolean hasNext() {
            return next != null;
        }

        abstract T eval(T node, String nodeName) throws ValueException;

        T eval(T node) throws ValueException {
            return eval(node, propertyName);
        }

        final void setNext(NodeEvaluator<T> next) {
            this.next = next;
        }

        final NodeEvaluator<T> next() {
            return next;
        }

        final T evalAndAdvance(T node, String nodeName) throws ValueException {
            T evaluated = eval(node, nodeName);
            if (next != null) {
                return next.evalAndAdvance(evaluated, nodeName);
            }
            return evaluated;
        }

        final T evalAndAdvance(T node) throws ValueException {
            T evaluated = eval(node);
            if (next != null) {
                return next.evalAndAdvance(evaluated);
            }
            return evaluated;
        }

        final T evalAndAdvanceRecursive(T node, String nodeName) throws ValueException {
            NodeEvaluator<T> current = this;

            while (current != null) {
                node = current.eval(node, nodeName);
                current = current.next;
            }

            return node;
        }

        final T evalAndAdvanceRecursive(T node) throws ValueException {
            return evalAndAdvanceRecursive(node, propertyName);
        }
    }

    private static class PropertyGetter<T extends Node<T>> extends NodeEvaluator<T> {

        PropertyGetter(String propertyName) {
            super(propertyName);
        }

        @Override
        public T eval(T node, String nodeName) {
            return node.getProperty(nodeName, propertyName);
        }
    }

    private static class ArrayGetter<T extends Node<T>> extends NodeEvaluator<T> {

        private final List<GeneralizedKey> indexes;

        ArrayGetter(String propertyName, List<GeneralizedKey> indexes) {
            super(propertyName);
            this.indexes = Objects.requireNonNull(indexes);
        }

        @Override
        public final T eval(T node, String nodeName) {
            T navigable = node.getProperty(nodeName, propertyName);
            for (GeneralizedKey gk : indexes) {
                navigable = gk.eval(navigable, nodeName);
            }
            return navigable;
        }

        @Override
        public final T eval(T node) {
            T navigable = node.getProperty(propertyName, propertyName);
            for (GeneralizedKey gk : indexes) {
                navigable = gk.eval(navigable);
            }
            return navigable;
        }
    }

    private static interface GeneralizedKey {

        static GeneralizedKey index(String container, int index) {
            return new Index(container, index);
        }

        static GeneralizedKey key(String container, String key) {
            return new Key(container, key);
        }

        <T extends Node<T>> T eval(Node<T> node);

        <T extends Node<T>> T eval(Node<T> node, String nodeName);

        String unboundNodeName();
    }

    static class Key implements GeneralizedKey {

        private final String key;

        Key(String container, String key) {
            this.key = key;
        }

        @Override
        public <T extends Node<T>> T eval(Node<T> node, String nodeName) {
            return node.getProperty(nodeName, key);
        }

        @Override
        public <T extends Node<T>> T eval(Node<T> node) {
            return eval(node, key);
        }

        @Override
        public String unboundNodeName() {
            return key;
        }
    }

    static class Index implements GeneralizedKey {

        private final int index;
        private final String container;
        private final String unboundNodeName;

        Index(String container, int index) {
            this.container = container;
            this.index = index;
            this.unboundNodeName = container + "[" + index + "]";
        }

        @Override
        public <T extends Node<T>> T eval(Node<T> node, String nodeName) {
            return node.getIndexed(nodeName, index, container);
        }

        @Override
        public <T extends Node<T>> T eval(Node<T> node) {
            return node.getIndexed(unboundNodeName, index, container);
        }

        @Override
        public String unboundNodeName() {
            return unboundNodeName;
        }
    }

    public static class SelectorExpressionParser<T extends Node<T>> {

        private static Pattern INDEXES = Pattern.compile("\\[(?:'([^']*)'|(\\d+))\\]");

        private static List<GeneralizedKey> parseIndexes(
                ParsingContext ctx, String field, String indexedExpression)
                throws ExtractionException {
            List<GeneralizedKey> indexes = new ArrayList<>();
            Matcher matcher = INDEXES.matcher(indexedExpression);
            String container = field;
            int previousEnd = 0;
            while (matcher.find()) {
                int currentStart = matcher.start();
                if (currentStart != previousEnd) {
                    throw ExtractionException.invalidIndexedExpression(ctx.expression());
                }
                previousEnd = matcher.end();
                String key = matcher.group(1);
                String index = matcher.group(2);
                GeneralizedKey gk =
                        key != null
                                ? GeneralizedKey.key(container, key)
                                : GeneralizedKey.index(container, Integer.valueOf(index));
                indexes.add(gk);
                container = gk.unboundNodeName();
            }
            if (previousEnd < indexedExpression.length()) {
                throw ExtractionException.invalidIndexedExpression(ctx.expression());
            }
            return indexes;
        }

        public SelectorExpressionParser() {}

        public NodeEvaluator<T> parse(ParsingContext ctx) throws ExtractionException {
            ctx.matchRoot();
            return parseTokens(ctx);
        }

        private NodeEvaluator<T> parseTokens(ParsingContext ctx) throws ExtractionException {
            NodeEvaluator<T> previous = null, head = null;
            while (ctx.hasNext()) {
                String token = ctx.next();
                if (token.isBlank()) {
                    throw ExtractionException.missingToken(ctx.expression());
                }

                NodeEvaluator<T> current = createEvaluator(ctx, token);
                if (previous != null) {
                    previous.setNext(current);
                } else {
                    head = current;
                }
                previous = current;
            }

            return head;
        }

        private NodeEvaluator<T> createEvaluator(ParsingContext ctx, String token)
                throws ExtractionException {
            int lbracket = token.indexOf('[');

            if (lbracket != -1) {
                String field = token.substring(0, lbracket);
                String indexedExpression = token.substring(lbracket);
                List<GeneralizedKey> indexes = parseIndexes(ctx, field, indexedExpression);
                if (indexes.isEmpty()) {
                    throw ExtractionException.invalidIndexedExpression(ctx.expression());
                }

                return new ArrayGetter<>(field, indexes);
            }

            return new PropertyGetter<>(token);
        }
    }
}
