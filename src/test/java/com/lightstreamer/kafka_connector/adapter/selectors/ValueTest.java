package com.lightstreamer.kafka_connector.adapter.selectors;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;

@Tag("unit")
public class ValueTest {

    @Test
    public void shouldReturnText() {
        Value value = Value.of("name", "value");
        assertThat(value.text()).isEqualTo("value");
    }

    @Test
    public void shouldMatch() {
        Value value1 = Value.of("name", "value");
        Value value2 = Value.of("name", "value");
        assertThat(value1.match(value2)).isTrue();
    }

    @Test
    public void shouldNotMatch() {
        Value value1 = Value.of("name", "value1");
        Value value2 = Value.of("name", "value2");
        assertThat(value1.match(value2)).isFalse();
    }

    public void shouldNotBeContainer() {
        assertThat(Value.of("name", "value").isContainer()).isFalse();
    }
}
