
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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConstantSelectorSupplier implements SelectorSupplier<ConstantSelector> {

    private static class ConstantSelectorImpl extends BaseSelector implements ConstantSelector {

        private final Constant constant;

        ConstantSelectorImpl(String name, Constant expression) {
            super(name, expression);
            this.constant = expression;
        }

        @Override
        public Data extract(KafkaRecord<?, ?> record, boolean checkScalar) throws ValueException {
            Object data =
                    switch (constant) {
                        case TIMESTAMP -> String.valueOf(record.timestamp());
                        case PARTITION -> String.valueOf(record.partition());
                        case OFFSET -> String.valueOf(record.offset());
                        case TOPIC -> record.topic();
                        case KEY -> record.key();
                        case VALUE -> record.value();
                        default ->
                                throw new IllegalStateException("Unexpected constant: " + constant);
                    };
            return new SimpleData(name(), Objects.toString(data, null));
        }

        @Override
        public Data extractKey(KafkaRecord<Object, ?> record, boolean checkScalar)
                throws ValueException {
            return new SimpleData(name(), Objects.toString(record.key(), null));
        }

        @Override
        public Data extractValue(KafkaRecord<?, Object> record, boolean checkScalar)
                throws ValueException {
            return new SimpleData(name(), Objects.toString(record.value(), null));
        }
    }

    private final Set<Constant> allowedConstants;

    ConstantSelectorSupplier(Constant... constant) {
        this.allowedConstants = new LinkedHashSet<>(Arrays.asList(constant));
    }

    ConstantSelectorSupplier() {
        this(Constant.values());
    }

    @Override
    public ConstantSelector newSelector(String name, ExtractionExpression expression)
            throws ExtractionException {
        return mkSelector(name, expression);
    }

    String expectedConstantStr() {
        return allowedConstants.stream().map(Constant::toString).collect(Collectors.joining("|"));
    }

    private ConstantSelectorImpl mkSelector(String name, ExtractionExpression expression)
            throws ExtractionException {
        if (!allowedConstants.contains(expression.constant())) {
            throw ExtractionException.expectedRootToken(name, expectedConstantStr());
        }
        if (expression.tokens().length > 1) {
            throw ExtractionException.notAllowedAttributes(name, expression.expression());
        }
        return new ConstantSelectorImpl(name, expression.constant());
    }

    public static ConstantSelectorSupplier KeySelector() {
        return new ConstantSelectorSupplier(Constant.KEY);
    }

    public static ConstantSelectorSupplier ValueSelector() {
        return new ConstantSelectorSupplier(Constant.VALUE);
    }
}
