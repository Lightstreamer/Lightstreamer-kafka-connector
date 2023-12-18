package com.lightstreamer.kafka_connector.adapter.selectors;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueContainer;

@Tag("unit")
public class ValueContainerTest {

    @Test
    public void shouldBeContainer() {
        Value e1 = Value.of("name", "value");
        ValueContainer container = ValueContainer.of("container", e1);
        assertThat(container.isContainer()).isTrue();
        assertThat(container.name()).isEqualTo("container");
        assertThat(container.values()).containsExactly(e1);
    }

    @Test
    public void shouldContainMoreValues() {
        ValueContainer container = ValueContainer
                .of("container",
                        Value.of("name1", "value1"),
                        Value.of("name2", "value2"),
                        Value.of("name3", "value3"));

        assertThat(container.values()).containsExactly(
                Value.of("name1", "value1"),
                Value.of("name2", "value2"),
                Value.of("name3", "value3"));
    }

    @Test
    public void shouldMatchValue() {
        ValueContainer container = ValueContainer
                .of("container",
                        Value.of("name1", "value1"),
                        Value.of("name2", "value2"),
                        Value.of("name3", "value3"));
        assertThat(container.matches(Value.of("name1", "value1"))).isTrue();
        assertThat(container.matches(Value.of("name2", "value2"))).isTrue();
        assertThat(container.matches(Value.of("name3", "value3"))).isTrue();
    }

    @Test
    public void shoulNotdMatchValue() {
        ValueContainer container = ValueContainer
                .of("container",
                        Value.of("name1", "value1"),
                        Value.of("name2", "value2"),
                        Value.of("name3", "value3"));
        assertThat(container.matches(Value.of("name1", "valueX"))).isFalse();
        assertThat(container.matches(Value.of("name", "value1"))).isFalse();
    }

    @Test
    public void shouldMatchContainer() {
        ValueContainer container1 = ValueContainer
                .of("container1",
                        Value.of("name1", "value1"),
                        Value.of("name2", "value2"));

        ValueContainer container2 = ValueContainer
                .of("container2",
                        Value.of("name1", "value1"),
                        Value.of("name2", "value2"));

        assertThat(container1.matches(container2)).isTrue();
    }

    @Test
    public void shouldNotMatchContainer() {
        ValueContainer container1 = ValueContainer
                .of("container1",
                        Value.of("name1", "valueX"),
                        Value.of("name2", "value2"),
                        Value.of("name3", "value3"));

        ValueContainer container2 = ValueContainer
                .of("container2",
                        Value.of("name1", "value1"),
                        Value.of("name2", "value2"),
                        Value.of("name3", "value3"));
        assertThat(container1.matches(container2)).isFalse();
    }
}
