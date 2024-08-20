
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

        ConstantSelectorImpl(String name, Constant constant) {
            super(name, constant);
        }

        @Override
        public Data extract(KafkaRecord<?, ?> record) {
            Constant constant = expression().constant();
            Object data =
                    switch (constant) {
                        case TIMESTAMP -> String.valueOf(record.timestamp());
                        case PARTITION -> String.valueOf(record.partition());
                        case OFFSET -> String.valueOf(record.offset());
                        case TOPIC -> record.topic();
                        case KEY -> record.key();
                        case VALUE -> record.value();
                    };
            return new SimpleData(name(), Objects.toString(data, null));
        }
    }

    private static class ConstantKeyValueSelector
            implements KeySelector<Object>, ValueSelector<Object> {

        private ConstantSelectorImpl name;

        protected ConstantKeyValueSelector(ConstantSelectorImpl name) {
            this.name = name;
        }

        @Override
        public Data extractKey(KafkaRecord<Object, ?> record) throws ValueException {
            return name.extract(record);
        }

        @Override
        public Data extractValue(KafkaRecord<?, Object> record) throws ValueException {
            return name.extract(record);
        }

        @Override
        public String name() {
            return name.name();
        }

        @Override
        public ExtractionExpression expression() {
            return name.expression();
        }
    }

    private final Set<Constant> allowedConstants;

    public ConstantSelectorSupplier(Constant... constant) {
        this.allowedConstants = new LinkedHashSet<>(Arrays.asList(constant));
    }

    public ConstantSelectorSupplier() {
        this(Constant.values());
    }

    @Override
    public ConstantSelector newSelector(String name, ExtractionExpression expression)
            throws ExtractionException {
        return mkSelector(name, expression);
    }

    public KeySelector<Object> newKeySelector(String name, ExtractionExpression expression)
            throws ExtractionException {
        return new ConstantKeyValueSelector(mkSelector(name, expression));
    }

    public ValueSelector<Object> newValueSelector(String name, ExtractionExpression expression)
            throws ExtractionException {
        return new ConstantKeyValueSelector(mkSelector(name, expression));
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
}
