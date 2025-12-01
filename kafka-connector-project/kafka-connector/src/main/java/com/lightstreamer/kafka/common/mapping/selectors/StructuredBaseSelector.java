
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
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.NodeEvaluator;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.ParsingContext;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class StructuredBaseSelector<T extends Node<T>> extends BaseSelector {

    private final Parsers.SelectorExpressionParser<T> parser =
            new Parsers.SelectorExpressionParser<>();

    private final NodeEvaluator<T> evaluator;

    protected StructuredBaseSelector(ExtractionExpression expression, Constant expectedRoot)
            throws ExtractionException {
        super(expression);
        this.evaluator = parser.parse(new ParsingContext(expression, expectedRoot));
    }

    protected final <P> Node<T> eval(
            Supplier<P> payloadSupplier, BiFunction<String, P, T> rootNode, boolean checkScalar) {

        return doEval(payloadSupplier, rootNode, n -> evaluator.evalAndAdvance(n), checkScalar);
    }

    protected final <P> Node<T> eval(
            String name,
            Supplier<P> payload,
            BiFunction<String, P, T> rootNode,
            boolean checkScalar) {

        return doEval(payload, rootNode, node -> evaluator.evalAndAdvance(node, name), checkScalar);
    }

    private <P> Node<T> doEval(
            Supplier<P> payload,
            BiFunction<String, P, T> rootNode,
            Function<T, T> eval,
            boolean checkScalar) {
        T node = Node.createRoot(payload, rootNode);
        node = eval.apply(node);

        if (checkScalar && !node.isScalar()) {
            throw ValueException.nonComplexObjectRequired(expression().expression());
        }

        return node;
    }

    protected final <P> void evalInto(
            Supplier<P> payloadSupplier,
            BiFunction<String, P, T> nodeFactory,
            Map<String, String> target) {
        eval(payloadSupplier, nodeFactory, false).flatIntoMap(target);
    }
}
