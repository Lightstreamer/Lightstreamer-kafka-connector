package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.MetaSelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;

@Tag("unit")
public class MetaSelectorTest {

    private static ConsumerRecord<String, String> record() {
        return new ConsumerRecord<>(
                "record-topic",
                150,
                120,
                ConsumerRecord.NO_TIMESTAMP,
                TimestampType.NO_TIMESTAMP_TYPE,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                "record-key",
                "record-value",
                new RecordHeaders(),
                Optional.empty());
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
                ATTRIBUTE,      VALUE
                TOPIC,          record-topic
                PARTITION,      150
                TIMESTAMP,      -1
            """)
    public void shouldExtractAttribute(String attributeName, String expectedValue) {
        MetaSelector r = MetaSelector.of("field_name", attributeName);
        Value value = r.extract(record());
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldNotExtractAttribute() {
        MetaSelector r = MetaSelector.of("field_name", "NOT-EXISTING-ATTRIBUTE");
        Value value = r.extract(record());
        assertThat(value.name()).isEqualTo("field_name");
        assertThat(value.text()).isEqualTo("Not-existing record attribute");
    }
}
