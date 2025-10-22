
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
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.GeneralizedKey;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.LinkedNodeEvaluator;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.NodeEvaluator;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.ParsingContext;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class StructuredBaseSelector<T extends Node<T>> extends BaseSelector {

    private static class PropertyGetter<T extends Node<T>> implements NodeEvaluator<T> {

        public static <T extends Node<T>> Node<T> get(String name, Node<T> node) {
            if (node.isNull()) {
                throw ValueException.nullObject(name);
            }

            if (node.isScalar()) {
                throw ValueException.scalarObject(name);
            }

            if (!node.has(name)) {
                throw ValueException.fieldNotFound(name);
            }
            return node.get(name);
        }

        private final String name;

        PropertyGetter(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Node<T> eval(Node<T> node) {
            return get(name, node);
        }
    }

    private static class ArrayGetter<T extends Node<T>> implements NodeEvaluator<T> {

        private final String name;
        private final PropertyGetter<T> getter;
        private final List<GeneralizedKey> indexes;

        ArrayGetter(String fieldName, List<GeneralizedKey> indexes) {
            this.name = Objects.requireNonNull(fieldName);
            this.indexes = Objects.requireNonNull(indexes);
            this.getter = new PropertyGetter<T>(name);
        }

        @Override
        public String name() {
            return name;
        }

        Node<T> get(int index, Node<T> node) {
            if (node.isNull()) {
                throw ValueException.nullObject(name, index);
            }
            if (node.isArray()) {
                if (index < node.size()) {
                    return node.get(index);
                } else {
                    throw ValueException.indexOfOutBounds(index);
                }
            } else {
                throw ValueException.noIndexedField(name);
            }
        }

        @Override
        public Node<T> eval(Node<T> node) {
            Node<T> result = getter.eval(node);
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

    private final Parsers.SelectorExpressionParser<T> parser =
            new Parsers.SelectorExpressionParser<>(PropertyGetter::new, ArrayGetter::new);

    private final LinkedNodeEvaluator<T> evaluator;

    protected StructuredBaseSelector(
            String name, ExtractionExpression expression, Constant expectedRoot)
            throws ExtractionException {
        super(name, expression);
        ParsingContext ctx = new ParsingContext(name, expression, expectedRoot);
        this.evaluator = parser.parse(ctx);
    }

    protected final <P> Data eval(
            Supplier<P> payloadSupplier, Function<P, Node<T>> nodeFactory, boolean checkScalar) {
        Node<T> node = Node.checkNull(payloadSupplier, nodeFactory);
        LinkedNodeEvaluator<T> current = evaluator;
        while (current != null) {
            node = current.eval(node);
            current = current.next();
        }

        if (checkScalar && !node.isScalar()) {
            throw ValueException.nonComplexObjectRequired(expression().expression());
        }

        return Data.from(name(), node.asText());
    }
}
