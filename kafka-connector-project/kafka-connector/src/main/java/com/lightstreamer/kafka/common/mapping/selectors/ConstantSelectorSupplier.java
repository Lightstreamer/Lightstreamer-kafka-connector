
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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConstantSelectorSupplier implements SelectorSupplier<ConstantSelector> {

    private static class ConstantSelectorImpl extends BaseSelector implements ConstantSelector {

        private final Constant constant;

        ConstantSelectorImpl(Constant expression) {
            super(expression);
            this.constant = expression;
        }

        @Override
        public Data extract(KafkaRecord<?, ?> record, boolean checkScalar) {
            return extract(constant.name(), record, checkScalar);
        }

        @Override
        public Data extract(String name, KafkaRecord<?, ?> record, boolean checkScalar)
                throws ValueException {
            return new SimpleData(name, getText(record));
        }

        private String getText(KafkaRecord<?, ?> record) {
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
            return Objects.toString(data, null);
        }

        @Override
        public Data extractKey(String name, KafkaRecord<Object, ?> record, boolean checkScalar)
                throws ValueException {
            return new SimpleData(name, Objects.toString(record.key(), null));
        }

        @Override
        public Data extractKey(KafkaRecord<Object, ?> record, boolean checkScalar)
                throws ValueException {
            return extractKey(constant.name(), record, checkScalar);
        }

        @Override
        public Data extractValue(String name, KafkaRecord<?, Object> record, boolean checkScalar)
                throws ValueException {
            return new SimpleData(name, Objects.toString(record.value(), null));
        }

        @Override
        public Data extractValue(KafkaRecord<?, Object> record, boolean checkScalar)
                throws ValueException {
            return extractValue(constant.name(), record, checkScalar);
        }

        @Override
        public void extractKeyInto(KafkaRecord<Object, ?> record, Map<String, String> target)
                throws ValueException {
            target.put(constant.name(), Objects.toString(record.key(), null));
        }

        @Override
        public void extractValueInto(KafkaRecord<?, Object> record, Map<String, String> target)
                throws ValueException {
            target.put(constant.name(), Objects.toString(record.value(), null));
        }

        @Override
        public void extractInto(KafkaRecord<?, ?> record, Map<String, String> target)
                throws ValueException {
            target.put(constant.name(), extract(record, true).text());
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
    public ConstantSelector newSelector(ExtractionExpression expression)
            throws ExtractionException {
        return mkSelector(expression);
    }

    String expectedConstantStr() {
        return allowedConstants.stream().map(Constant::toString).collect(Collectors.joining("|"));
    }

    private ConstantSelectorImpl mkSelector(ExtractionExpression expression)
            throws ExtractionException {
        if (!allowedConstants.contains(expression.constant())) {
            throw ExtractionException.expectedRootToken(
                    expression.expression(), expectedConstantStr());
        }
        if (expression.tokens().length > 1) {
            throw ExtractionException.notAllowedAttributes(expression.expression());
        }
        return new ConstantSelectorImpl(expression.constant());
    }

    public static ConstantSelectorSupplier KeySelector() {
        return new ConstantSelectorSupplier(Constant.KEY);
    }

    public static ConstantSelectorSupplier ValueSelector() {
        return new ConstantSelectorSupplier(Constant.VALUE);
    }
}
