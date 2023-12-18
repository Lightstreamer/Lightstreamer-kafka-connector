package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.consumers.RemappedRecord;

public class ConsumedRecordTest {

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("provider")
    public void shouldExtractSubValues(Map<String, String> values, Set<String> keys, Map<String, String> expected) {
        RemappedRecord r = new RemappedRecord("topic", values);
        Map<String, String> subMap = r.subValues(keys);
        assertThat(subMap).isEqualTo(expected);
    }

    static Stream<Arguments> provider() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(),
                        Collections.emptySet(),
                        Collections.emptyMap()),
                arguments(
                        Collections.emptyMap(),
                        Set.of("a"),
                        Collections.emptyMap()),
                arguments(
                        Map.of("a", "A"),
                        Set.of("a"),
                        Map.of("a", "A")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Set.of("a"),
                        Map.of("a", "A")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Set.of("a", "b"),
                        Map.of("a", "A", "b", "B")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Set.of("a", "b", "c"),
                        Map.of("a", "A", "b", "B")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Set.of("c", "d"),
                        Collections.emptyMap()));

    }
}
