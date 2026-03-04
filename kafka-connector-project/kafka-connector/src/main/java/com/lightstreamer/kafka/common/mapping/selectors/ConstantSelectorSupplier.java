
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

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.HEADERS;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.records.KafkaRecord;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConstantSelectorSupplier implements SelectorSupplier<GenericSelector> {

    private static class ConstantSelectorImpl extends BaseSelector implements GenericSelector {

        private final Constant constant;

        ConstantSelectorImpl(Constant constant) {
            super(constant);
            this.constant = constant;
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
                                // This should never happen as HEADERS is not supported
                                throw new IllegalStateException("Unexpected constant: " + constant);
                    };
            return Objects.toString(data, null);
        }
    }

    private final Set<Constant> allowedConstants;

    private ConstantSelectorSupplier(Constant... constant) {
        this.allowedConstants = new LinkedHashSet<>(Arrays.asList(constant));
    }

    @Override
    public GenericSelector newSelector(ExtractionExpression expression) throws ExtractionException {
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

    public static ConstantSelectorSupplier makeSelectorSupplier(Constant... constants) {
        for (Constant constant : constants) {
            if (constant == HEADERS) {
                throw new IllegalArgumentException("Cannot handle HEADERS constant");
            }
        }
        return new ConstantSelectorSupplier(constants);
    }
}
