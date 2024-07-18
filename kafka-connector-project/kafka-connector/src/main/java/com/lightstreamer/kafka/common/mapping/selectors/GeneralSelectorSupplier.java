
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

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class GeneralSelectorSupplier implements SelectorSupplier<GeneralSelector> {

    private static class GeneralKeySelector extends BaseSelector implements KeySelector<Object> {

        private final GeneralSelector inner;

        GeneralKeySelector(GeneralSelector inner) {
            super(inner.name(), inner.expression());
            this.inner = inner;
        }

        @Override
        public Value extract(KafkaRecord<Object, ?> record) throws ValueException {
            return inner.extract(record);
        }
    }

    private static class GeneralValueSelector extends BaseSelector
            implements ValueSelector<Object> {

        private final GeneralSelector inner;

        GeneralValueSelector(GeneralSelector inner) {
            super(inner.name(), inner.expression());
            this.inner = inner;
        }

        @Override
        public Value extract(KafkaRecord<?, Object> record) throws ValueException {
            return inner.extract(record);
        }
    }

    private static class GeneralSelectorImpl extends BaseSelector implements GeneralSelector {

        private final Constant constant;

        GeneralSelectorImpl(String name, Constant constant) {
            super(name, constant.toString());
            this.constant = constant;
        }

        @Override
        public Value extract(KafkaRecord<?, ?> record) {
            Object value =
                    switch (constant) {
                        case TIMESTAMP -> String.valueOf(record.timestamp());
                        case PARTITION -> String.valueOf(record.partition());
                        case OFFSET -> String.valueOf(record.offset());
                        case TOPIC -> record.topic();
                        case KEY -> record.key();
                        case VALUE -> record.value();
                    };
            return new SimpleValue(name(), Objects.toString(value, null));
        }
    }

    private final Constant[] allowedConstants;

    public GeneralSelectorSupplier(Constant... constant) {
        this.allowedConstants = constant;
    }

    public GeneralSelectorSupplier() {
        this(Constant.values());
    }

    @Override
    public GeneralSelector newSelector(String name, String expression) throws ExtractionException {
        Constant constant = Constant.from(expression);
        if (constant == null) {
            throw ExtractionException.expectedRootToken(name, expectedConstantStr());
        }
        return new GeneralSelectorImpl(name, constant);
    }

    String expectedConstantStr() {
        return Arrays.stream(allowedConstants)
                .map(a -> a.toString())
                .collect(Collectors.joining("|"));
    }

    public KeySelector<Object> newKeySelectorSelector(String name, String expression)
            throws ExtractionException {
        return new GeneralKeySelector(newSelector(name, expression));
    }

    public ValueSelector<Object> newValueSelectorSelector(String name, String expression)
            throws ExtractionException {
        return new GeneralValueSelector(newSelector(name, expression));
    }
}
