package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema.SchemaName;

public class MappedRecordTest {

    private static Set<Value> toValues(String schemaName, Map<String, String> values) {
        return values.entrySet()
                .stream()
                .map(e -> Value.of(SchemaName.of(schemaName), e))
                .collect(Collectors.toSet());
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("provider")
    public void shouldFilter(Map<String, String> values, Schema schema, Map<String, String> expected) {
        MappedRecord mapped = new DefaultMappedRecord("topic", toValues("test", values));

        assertThat(mapped.topic()).isEqualTo("topic");
        assertThat(mapped.filter(schema)).isEqualTo(expected);
    }

    @Test
    public void shouldFilterFromDifferentSchemas() {
        Set<Value> tag1Set = toValues("schema1", Map.of("a", "A"));
        Set<Value> tag2Set = toValues("schema2", Map.of("a", "B"));

        Set<Value> allValues = Stream.concat(tag1Set.stream(), tag2Set.stream()).collect(Collectors.toSet());

        MappedRecord mapped = new DefaultMappedRecord("topic", allValues);

        assertThat(mapped.filter(Schema.of(SchemaName.of("schema1"), "a", "c"))).containsExactly("a", "A");
        assertThat(mapped.filter(Schema.of(SchemaName.of("schema2"), "a", "c"))).containsExactly("a", "B");
    }

    static Stream<Arguments> provider() {
        return Stream.of(
                arguments(
                        Collections.emptyMap(),
                        Schema.empty("test"),
                        Collections.emptyMap()),
                arguments(
                        Collections.emptyMap(),
                        Schema.of(SchemaName.of("test"), "a"),
                        Collections.emptyMap()),
                arguments(
                        Map.of("a", "A"),
                        Schema.of(SchemaName.of("test"), "a"),
                        Map.of("a", "A")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of(SchemaName.of("test"), "a"),
                        Map.of("a", "A")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of(SchemaName.of("test"), "a", "b"),
                        Map.of("a", "A", "b", "B")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of(SchemaName.of("test"), "a", "b", "c"),
                        Map.of("a", "A", "b", "B")),
                arguments(
                        Map.of("a", "A", "b", "B"),
                        Schema.of(SchemaName.of("test"), "c", "d"),
                        Collections.emptyMap()));

    }
}
