
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

package com.lightstreamer.kafka_connector.adapters.mapping.selectors;

import com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaSelectorSupplier implements SelectorSupplier<MetaSelector> {

    static enum Attribute {
        TIMESTAMP {
            @Override
            String value(ConsumerRecord<?, ?> record) {
                return String.valueOf(record.timestamp());
            }
        },

        PARTITION {
            @Override
            String value(ConsumerRecord<?, ?> record) {
                return String.valueOf(record.partition());
            }
        },

        OFFSET {
            @Override
            String value(ConsumerRecord<?, ?> record) {
                return String.valueOf(record.offset());
            }
        },

        TOPIC {
            @Override
            String value(ConsumerRecord<?, ?> record) {
                return record.topic();
            }
        },

        NULL {
            @Override
            String value(ConsumerRecord<?, ?> record) {
                return NOT_EXISTING_RECORD_ATTRIBUTE;
            }
        };

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

        abstract String value(ConsumerRecord<?, ?> record);

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
    public Value extract(ConsumerRecord<?, ?> record) {
        return new SimpleValue(name(), attribute.value(record));
    }
}
