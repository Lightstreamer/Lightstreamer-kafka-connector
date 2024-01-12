package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import static com.google.common.truth.Truth.assertThat;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.test_utils.SelectorsSuppliers;

@Tag("unit")
public class ValueTest {

    @Test
    public void shouldCreateValue() {
        Value value = Value.of("name", "value");

        assertThat(value.text()).isEqualTo("value");
        assertThat(value.name()).isEqualTo("name");
    }

    @Test
    public void shouldCreateValuesContainer() {
        Selectors<String, String> selectors = Selectors.from(
                SelectorsSuppliers.string(), "schema", Map.of("name", "VALUE"));
        ValuesContainer container = new DefaultValuesContainer(selectors, Set.of(Value.of("name", "aValue")));

        assertThat(container.selectors()).isSameInstanceAs(selectors);
        assertThat(container.values()).containsExactly(Value.of("name", "aValue"));
    }
}
