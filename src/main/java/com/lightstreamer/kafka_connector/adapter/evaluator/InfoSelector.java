package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InfoSelector extends BaseSelector<ConsumerRecord<?, ?>> {

    private static Logger log = LoggerFactory.getLogger(InfoSelector.class);

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

    // Pattern EXPR = Pattern.compile("\\$\\{(.*)\\}");
    private static final Pattern EXPR = Pattern.compile("(.*)");

    private final Attribute attribute;

    public InfoSelector(String name, String expression) {
        super(name, expression);
        Matcher matcher = EXPR.matcher(expression);
        if (matcher.matches()) {
            String group = matcher.group(1);
            log.info("Attribute: {}", group);
            attribute = Attribute.of(group);
        } else {
            // attribute = Attribute.NULL.toString();
            attribute = null;
        }
    }

    @Override
    public Value extract(ConsumerRecord<?, ?> record) {
        return Value.of(name(), attribute.value(record));
    }
}
