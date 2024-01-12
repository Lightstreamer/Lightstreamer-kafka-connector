package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.record;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;

@Tag("unit")
public class MetaSelectorTest {

    static MetaSelector metaSelector(String expression) {
        return new MetaSelectorSupplier().newSelector("field_name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
                EXPRESSION,     VALUE
                TOPIC,          record-topic
                PARTITION,      150
                TIMESTAMP,      -1
            """)
    public void shouldExtractAttribute(String expression, String expectedValue) {
        MetaSelector selector = metaSelector(expression);
        Value value = selector.extract(record("record-key", "record-value"));
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
                EXPRESSION               |   EXPECTED_ERROR_MESSAGE
                NOT-EXISTING-ATTRIBUTE   |   Expected <on of [TIMESTAMP, PARTITION, TOPIC]>
                ""                       |   Expected <on of [TIMESTAMP, PARTITION, TOPIC]>
                PARTITION.               |   Expected <on of [TIMESTAMP, PARTITION, TOPIC]>
            """)
    public void shouldNotCreate(String expression, String expectedErrorMessage) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> metaSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
