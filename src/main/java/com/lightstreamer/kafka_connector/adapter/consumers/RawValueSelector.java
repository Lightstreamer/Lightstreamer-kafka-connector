package com.lightstreamer.kafka_connector.adapter.consumers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.evaluator.BaseValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

public class RawValueSelector extends BaseValueSelector<String, String> {

    protected static Logger log = LoggerFactory.getLogger(RawValueSelector.class);

    private static enum Attribute {
        KEY {
            @Override
            String value(ConsumerRecord<String, String> record) {
                return record.key();
            }
        },

        VALUE {
            @Override
            String value(ConsumerRecord<String, String> record) {
                return record.value();
            }
        },

        TIMESTAMP {
            @Override
            String value(ConsumerRecord<String, String> record) {
                return String.valueOf(record.timestamp());

            }
        },

        PARTITION {
            @Override
            String value(ConsumerRecord<String, String> record) {
                return String.valueOf(record.partition());
            }
        },

        TOPIC {

            @Override
            String value(ConsumerRecord<String, String> record) {
                return record.topic();
            }
        },

        NULL {

            @Override
            String value(ConsumerRecord<String, String> record) {
                return NOT_EXISTING_RECORD_ATTRIBUTE;
            }
        };

        static final Attribute[] values = Attribute.values();

        private static final String NOT_EXISTING_RECORD_ATTRIBUTE = "Not-existing record attribute";

        static Attribute of(String attributeName) {
            for (int i = 0; i < Attribute.values.length; i++) {
                if (values[i].toString().equals(attributeName)) {
                    return values[i];
                }

            }
            return NULL;
        }

        abstract String value(ConsumerRecord<String, String> record);

    }

    Pattern EXPR = Pattern.compile("\\$\\{(.*)\\}");

    private final String attribute;

    public RawValueSelector(String name, String expression) {
        super(name, expression);
        log.info("Creaated RawValueSelector for field <{}>", name);
        Matcher matcher= EXPR.matcher(expression);
        if (matcher.matches()) {
            attribute = matcher.group(1);
        } else {
            attribute = Attribute.NULL.toString();
        }

    }

    @Override
    public Value extract(ConsumerRecord<String, String> record) {
        return Value.of(name(), Attribute.of(attribute).value(record));
    }

}
