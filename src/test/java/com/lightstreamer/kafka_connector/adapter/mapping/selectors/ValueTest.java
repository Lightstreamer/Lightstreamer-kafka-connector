package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class ValueTest {

    @Test
    public void trivial() {
        Value value = Value.of("tag", "name", "value");
        assertThat(value.text()).isEqualTo("value");
        assertThat(value.tag()).isEqualTo("tag");
        assertThat(value.name()).isEqualTo("name");
    }

}
