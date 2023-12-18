package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface MetaSelector extends Selector {

    Value extract(ConsumerRecord<?, ?> record);

    static MetaSelector of(String name, String expression) {
        return new DefaultMetaSelector(name, expression);
    }

}

class DefaultMetaSelector extends BaseSelector implements MetaSelector {

    protected static Logger log = LoggerFactory.getLogger(DefaultMetaSelector.class);

    private static enum Attribute {
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
    }

    private final Attribute attribute;

    public DefaultMetaSelector(String name, String expression) {
        super(name, expression);
        attribute = Attribute.of(expression);
    }

    @Override
    public Value extract(ConsumerRecord<?, ?> record) {
        return Value.of(name(), attribute.value(record));
    }
}
