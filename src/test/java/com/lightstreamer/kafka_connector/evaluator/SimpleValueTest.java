package com.lightstreamer.kafka_connector.evaluator;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.evaluator.SimpleValue;
import com.lightstreamer.kafka_connector.adapter.evaluator.Value;

public class SimpleValueTest {

    @Test
    public void shouldReturnText() {
        Value value = new SimpleValue("name", "value");
        assertThat(value.text()).isEqualTo("value");
    }

    @Test
    public void shouldMatch() {
        Value value1 = new SimpleValue("name", "value");
        Value value2 = new SimpleValue("name", "value");
        assertThat(value1.match(value2)).isTrue();
    }

    @Test
    public void shouldNotMatch() {
        Value value1 = new SimpleValue("name", "value1");
        Value value2 = new SimpleValue("name", "value2");
        assertThat(value1.match(value2)).isFalse();
    }

    public void shouldNotBeContainer() {
        assertThat(new SimpleValue("name", "value").isContainer()).isFalse();
    }
}
