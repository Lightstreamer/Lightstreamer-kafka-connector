package com.lightstreamer.kafka_connector.adapter.consumers.raw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.evaluator.BaseSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

public class RawValueSelector2 extends BaseSelector<ConsumerRecord<?, ?>> {

    protected static Logger log = LoggerFactory.getLogger(RawValueSelector.class);

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

        KEY {
            String value(ConsumerRecord<?, ?> record) {
                return String.valueOf(record.key());
            }
        },

        VALUE {
            String value(ConsumerRecord<?, ?> record) {
                return String.valueOf(record.value());
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

    public RawValueSelector2(String name, String expression) {
        super(name, expression);
        attribute = Attribute.of(expression);
    }

    @Override
    public Value extract(ConsumerRecord<?, ?> record) {
        return Value.of(name(), attribute.value(record));
    }
}
