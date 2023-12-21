package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;

public class MappedRecordTest {

    private static Set<Value> toValues(Map<String, String> values) {
        return values.entrySet()
                .stream()
                .map(Value::of)
                .collect(Collectors.toSet());
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("provider")
    public void shouldFilter(Map<String, String> values, Schema schema, Map<String, String> expected) {
        MappedRecord mapped = new DefaultMappedRecord("topic", toValues(values));
        assertThat(mapped.topic()).isEqualTo("topic");
        Map<String, String> subMap = mapped.filter(schema);
        assertThat(subMap).isEqualTo(expected);
    }

    static Stream<Arguments> provider() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(),
                        Schema.empty(),
                        Collections.emptyMap()),
                arguments(
                        Collections.emptyMap(),
                        Schema.of("a"),
                        Collections.emptyMap()),
                arguments(
                        Map.of("a", "A"),
                        Schema.of("a"),
                        Map.of("a", "A")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of("a"),
                        Map.of("a", "A")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of("a", "b"),
                        Map.of("a", "A", "b", "B")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of("a", "b", "c"),
                        Map.of("a", "A", "b", "B")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of("c", "d"),
                        Collections.emptyMap()));

    }
}
