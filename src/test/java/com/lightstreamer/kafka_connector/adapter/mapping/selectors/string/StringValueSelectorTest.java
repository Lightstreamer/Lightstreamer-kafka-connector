package com.lightstreamer.kafka_connector.adapter.mapping.selectors.string;

import static com.google.common.truth.Truth.assertThat;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;

@Tag("unit")
public class StringValueSelectorTest {

    private static ConsumerRecord<String, ?> recordWithKey(String key) {
        return record(key, null);
    }

    private static ConsumerRecord<?, String> recordWithValue(String value) {
        return record(null, value);
    }

    private static ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>(
                "record-topic",
                150,
                120,
                ConsumerRecord.NO_TIMESTAMP,
                TimestampType.NO_TIMESTAMP_TYPE,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                key,
                value,
                new RecordHeaders(),
                Optional.empty());
    }

    static ValueSelector<String> valueSelector(String expression) {
        return new StringValueSelectorSupplier().selector("name", expression);
    }

    static KeySelector<String> keySelector(String expression) {
        return new StringKeySelectorSupplier().selector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION, EXPECTED
            VALUE,      joe
            VALUE,      alex
            """)
    public void shouldExtractValue(String expression, String expectedValue) {
        ValueSelector<String> selector = valueSelector(expression);
        assertThat(selector.name()).isEqualTo("name");
        assertThat(selector.extract(recordWithValue(expectedValue)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION, EXPECTED
            KEY,        joe
            KEY,        alex
            """)
    public void shouldExtractKey(String expression, String expectedValue) {
        KeySelector<String> selector = keySelector(expression);
        assertThat(selector.name()).isEqualTo("name");
        assertThat(selector.extract(recordWithKey(expectedValue)).text()).isEqualTo(expectedValue);
    }
}
