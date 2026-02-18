
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
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node.KafkaRecordNode;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.NodeEvaluator;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.ParsingContext;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public abstract class StructuredBaseSelector<P, T extends Node<T>> extends BaseSelector {

    private final Parsers.SelectorExpressionParser<T> parser =
            new Parsers.SelectorExpressionParser<>();

    private final BiFunction<String, P, T> rootNodeFactory;
    protected final NodeEvaluator<T> evaluator;

    protected StructuredBaseSelector(
            ExtractionExpression expression,
            Constant expectedRoot,
            BiFunction<String, P, T> rootNodeFactory)
            throws ExtractionException {
        super(expression);
        this.rootNodeFactory = rootNodeFactory;
        this.evaluator = parser.parse(new ParsingContext(expression, expectedRoot));
    }

    protected final Node<T> eval(Supplier<P> payloadSupplier, boolean checkScalar) {
        Node<T> recordNode = new KafkaRecordNode<>(payloadSupplier, this.rootNodeFactory);
        Node<T> resultNode = evaluator.evaluateChain(recordNode);

        if (checkScalar && !resultNode.isScalar()) {
            throw ValueException.nonComplexObjectRequired(expression().expression());
        }

        return resultNode;
    }

    protected final Node<T> eval(String name, Supplier<P> payloadSupplier, boolean checkScalar) {
        Node<T> recordNode = new KafkaRecordNode<>(payloadSupplier, this.rootNodeFactory);
        Node<T> resultNode = evaluator.evaluateChain(recordNode, name);

        if (checkScalar && !resultNode.isScalar()) {
            throw ValueException.nonComplexObjectRequired(expression().expression());
        }

        return resultNode;
    }

    protected final void evalInto(Supplier<P> payloadSupplier, Map<String, String> target) {
        eval(payloadSupplier, false).flatIntoMap(target);
    }
}
