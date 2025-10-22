
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
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.NodeEvaluator;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.ParsingContext;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public abstract class StructuredBaseSelector<T extends Node<T>> extends BaseSelector {

    private final Parsers.SelectorExpressionParser<T> parser =
            new Parsers.SelectorExpressionParser<>();

    private final NodeEvaluator<T> evaluator;

    protected StructuredBaseSelector(
            String name, ExtractionExpression expression, Constant expectedRoot)
            throws ExtractionException {
        super(name, expression);
        this.evaluator = parser.parse(new ParsingContext(name, expression, expectedRoot));
    }

    protected final <P> Data eval(
            Supplier<P> payloadSupplier,
            BiFunction<String, P, T> nodeFactory,
            boolean checkScalar) {
        Node<T> node = Node.checkNull(payloadSupplier, nodeFactory);
        NodeEvaluator<T> current = evaluator;

        while (current != null) {
            node = current.eval(node);
            current = current.next();
        }

        if (checkScalar && !node.isScalar()) {
            throw ValueException.nonComplexObjectRequired(expression().expression());
        }

        String dataName = name();
        String name = "*".equals(dataName) ? node.name() : dataName;
        // return Data.from(name, node.text());
        return node;
    }

    protected final <P> Collection<Data> evalMulti(
            Supplier<P> payloadSupplier, BiFunction<String, P, T> nodeFactory) {
        Node<T> node = Node.checkNull(payloadSupplier, nodeFactory);
        NodeEvaluator<T> current = evaluator;
        while (current != null) {
            node = current.eval(node);
            current = current.next();
        }

        return node.toData();
    }
}
