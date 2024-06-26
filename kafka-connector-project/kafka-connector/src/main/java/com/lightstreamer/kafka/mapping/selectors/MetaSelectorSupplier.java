
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

package com.lightstreamer.kafka.mapping.selectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class MetaSelectorSupplier implements SelectorSupplier<MetaSelector> {

    static enum Attribute {
        TIMESTAMP,
        PARTITION,
        OFFSET,
        TOPIC,
        NULL;

        private static final Attribute[] values = Attribute.values();

        private static final String NOT_EXISTING_RECORD_ATTRIBUTE = "Not-existing record attribute";

        static Attribute of(String attributeName) {
            for (int i = 0; i < Attribute.values.length; i++) {
                if (values[i].toString().equals(attributeName)) {
                    return values[i];
                }
            }
            return NULL;
        }

        String value(KafkaRecord<?, ?> record) {
            return switch (this) {
                case TIMESTAMP -> String.valueOf(record.timestamp());
                case PARTITION -> String.valueOf(record.partition());
                case OFFSET -> String.valueOf(record.offset());
                case TOPIC -> record.topic();
                case NULL -> NOT_EXISTING_RECORD_ATTRIBUTE;
            };
        }

        static List<Attribute> validAttributes() {
            return List.of(TIMESTAMP, PARTITION, TOPIC, OFFSET);
        }
    }

    @Override
    public MetaSelector newSelector(String name, String expression) {
        if (!maySupply(expression)) {
            ExpressionException.throwExpectedRootToken(
                    name,
                    Attribute.validAttributes().stream()
                            .map(a -> a.toString())
                            .collect(Collectors.joining("|")));
        }
        return new DefaultMetaSelector(name, expression);
    }

    @Override
    public boolean maySupply(String expression) {
        return Attribute.of(expression) != Attribute.NULL;
    }
}

class DefaultMetaSelector extends BaseSelector implements MetaSelector {

    protected static Logger log = LoggerFactory.getLogger(DefaultMetaSelector.class);

    private final MetaSelectorSupplier.Attribute attribute;

    public DefaultMetaSelector(String name, String expression) {
        super(name, expression);
        attribute = MetaSelectorSupplier.Attribute.of(expression);
    }

    @Override
    public Value extract(KafkaRecord<?, ?> record) {
        return new SimpleValue(name(), attribute.value(record));
    }
}
