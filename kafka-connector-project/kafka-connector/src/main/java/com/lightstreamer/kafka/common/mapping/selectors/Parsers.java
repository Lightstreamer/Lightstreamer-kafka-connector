
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
import com.lightstreamer.kafka.common.utils.Either;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parsers {

    public interface Node<T extends Node<T>> extends Data {

        String name();

        default T self() {
            @SuppressWarnings("unchecked")
            T self = (T) this;
            return self;
        }

        default T getProperty(String propertyName) {
            if (isNull()) {
                throw ValueException.nullObject(propertyName);
            }

            if (isScalar()) {
                throw ValueException.scalarObject(propertyName);
            }

            if (!has(propertyName)) {
                throw ValueException.fieldNotFound(propertyName);
            }
            return get(propertyName);
        }

        default boolean isNull() {
            return false;
        }

        boolean isScalar();

        boolean has(String propertyname);

        T get(String propertyName);

        default T getIndexed(int index) {
            if (isNull()) {
                throw ValueException.nullObject(name(), index);
            }

            // if (isScalar()) {
            //     throw ValueException.scalarObject(index);
            // }

            if (isArray()) {
                if (index < size()) {
                    return get(index);
                } else {
                    throw ValueException.indexOfOutBounds(index);
                }
            }

            throw ValueException.noIndexedField(name());
        }

        boolean isArray();

        int size();

        T get(int index);

        String text();

        default void visit(Consumer<Data> visitor) {}

        default List<Data> toData() {
            List<Data> result = new ArrayList<>();
            visit(result::add);
            return result;
        }

        static <K, T extends Node<T>> Node<T> checkNull(
                Supplier<K> obj, BiFunction<String, K, T> nodeFactory) {
            K object = obj.get();
            if (object == null) {
                return nullNode();
            }
            return rootNode(propertyName -> nodeFactory.apply(propertyName, object));
        }

        abstract class BaseNode<T extends BaseNode<T>> implements Node<T> {

            private final String name;

            BaseNode(String name) {
                this.name = Objects.requireNonNull(name);
            }

            @Override
            public String name() {
                return name;
            }
        }

        @SuppressWarnings("unchecked")
        static <T extends Node<T>> Node<T> nullNode() {
            return (Node<T>) NullNode.INSTANCE;
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
            public T get(String propertyName) {
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
            public T get(int index) {
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
            public T get(String propertyName) {
                return rootNodeFactory.apply(propertyName);
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
            public T get(int index) {
                return null;
            }

            @Override
            public String text() {
                return "ROOT";
            }
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

        private final String propertyName;
        private NodeEvaluator<T> next;

        NodeEvaluator(String name) {
            this.propertyName = Objects.requireNonNull(name);
        }

        final String propertyName() {
            return propertyName;
        }

        final boolean hasNext() {
            return next != null;
        }

        abstract T eval(Node<T> node) throws ValueException;

        final void setNext(NodeEvaluator<T> next) {
            this.next = next;
        }

        NodeEvaluator<T> next() {
            return next;
        }
    }

    private static class PropertyGetter<T extends Node<T>> extends NodeEvaluator<T> {

        static <T extends Node<T>> T get(String name, Node<T> node) {
            return node.getProperty(name);
        }

        PropertyGetter(String propertyName) {
            super(propertyName);
        }

        @Override
        public T eval(Node<T> node) {
            return get(propertyName(), node);
        }
    }

    private static class ArrayGetter<T extends Node<T>> extends NodeEvaluator<T> {

        private final List<GeneralizedKey> indexes;

        ArrayGetter(String propertyName, List<GeneralizedKey> indexes) {
            super(propertyName);
            this.indexes = Objects.requireNonNull(indexes);
        }

        T get(int index, Node<T> node) {
            return node.getIndexed(index);
        }

        @Override
        public T eval(Node<T> node) {
            T result = node.getProperty(propertyName());
            for (GeneralizedKey i : indexes) {
                if (i.isIndex()) {
                    result = get(i.index(), result);
                } else {
                    result = PropertyGetter.get(i.key(), result);
                }
            }
            return result;
        }
    }

    private static class GeneralizedKey {

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

        boolean isKey() {
            return genericKey.isLeft();
        }

        String key() {
            return genericKey.getLeft();
        }

        boolean isIndex() {
            return genericKey.isRight();
        }

        int index() {
            return genericKey.getRight();
        }

        public String toString() {
            return isKey() ? "Key = [%s]".formatted(key()) : "Index = [%d]".formatted(index());
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
                    throw ExtractionException.missingToken(ctx.name(), ctx.expression());
                }

                NodeEvaluator<T> current = createEvaluator(ctx, token);
                if (previous != null) {
                    previous.next = current;
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
                String indexedExpression = token.substring(lbracket);
                List<GeneralizedKey> indexes = parseIndexes(ctx, indexedExpression);
                if (indexes.isEmpty()) {
                    throw ExtractionException.invalidIndexedExpression(
                            ctx.name(), ctx.expression());
                }
                String field = token.substring(0, lbracket);
                return new ArrayGetter<>(field, indexes);
            }

            return new PropertyGetter<>(token);
        }
    }
}
