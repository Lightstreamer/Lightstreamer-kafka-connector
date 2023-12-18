package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.lightstreamer.kafka_connector.adapter.mapping.RecordInspector;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordInspector.Builder;

public class RecordInspectorBuilderTest {

    static Stream<Builder<?, ?>> builderProvider() {
        return Stream.of(RecordInspector.noSelectorsBuilder(), RecordInspector.stringSelectorsBuilder());
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("builderProvider")
    public void shouldBuildNotInstructedInspector(Builder<?, ?> builder) {
        RecordInspector<?, ?> inspector = builder.build();
        assertThat(inspector.names()).isEmpty();
    }

    @Test
    public void shouldNotInstructInspector() {
        Builder<?, ?> builder = RecordInspector.noSelectorsBuilder();
        Exception exception = assertThrows(RuntimeException.class, () -> builder.instruct("name", "unknown"));
        assertThat(exception.getMessage()).isEqualTo("Unknown expression <unknown>");
    }

    @Test
    public void shouldAddKeySelector() {
        Builder<String, String> rawBuilder = RecordInspector.stringSelectorsBuilder();
        boolean added = rawBuilder.addKeySelector("name", "KEY");
        assertThat(added).isTrue();

        // No duplicates
        assertThrows(RuntimeException.class, () -> rawBuilder.addKeySelector("name", "KEY"));
        assertThrows(RuntimeException.class, () -> rawBuilder.addValueSelector("name", "VALUE"));
        assertThrows(RuntimeException.class, () -> rawBuilder.addMetaSelector("name", "TOPIC"));
    }

    @Test
    public void shouldNotAddKeySelector() {
        Builder<?, ?> rawBuilder = RecordInspector.noSelectorsBuilder();
        boolean added = rawBuilder.addKeySelector("name", "KEY");
        assertThat(added).isFalse();

        rawBuilder.addKeySelector("name", "WRONG_EXPRESSION");
        assertThat(added).isFalse();
    }

    @Test
    public void shouldAddValueSelector() {
        Builder<String, String> rawBuilder = RecordInspector.stringSelectorsBuilder();
        boolean added = rawBuilder.addValueSelector("name", "VALUE");
        assertThat(added).isTrue();

        // No duplicates
        added = rawBuilder.addValueSelector("name", "VALUE");
        assertThat(added).isFalse();
    }

    @Test
    public void shouldNotAddValueSelector() {
        Builder<?, ?> rawBuilder = RecordInspector.stringSelectorsBuilder();
        boolean added = rawBuilder.addValueSelector("name", "VALUE");
        assertThat(added).isFalse();

        // No wrong expressions
        rawBuilder.addValueSelector("name", "WRONG_EXPRESSION");
        assertThat(added).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "TIMESTAMP", "TOPIC", "PARTITION" })
    public void shouldAddMetaSelector(String expression) {
        Builder<?, ?> builder1 = RecordInspector.noSelectorsBuilder();
        Builder<?, ?> builder2 = RecordInspector.stringSelectorsBuilder();

        List.of(builder1, builder2).forEach(builder -> {
            boolean added = builder.addMetaSelector("name", expression);
            assertThat(added).isTrue();

            // No duplicates
            added = builder.addMetaSelector("name", expression);
            assertThat(added).isFalse();
        });
    }

    @Test
    public void shouldInstruct() {
        Builder<String, String> builder = RecordInspector.stringSelectorsBuilder();
        builder.instruct("name1", "VALUE");
        builder.instruct("name1", "VALUE");
        builder.instruct("name1", "KEY");
        builder.instruct("name3", "TIMESTAMP");
        builder.instruct("name4", "TOPIC");
        builder.instruct("name5", "PARTITION");
    }

    // @ParameterizedTest
    // @ValueSource(strings = { "TIMESTAMP", "TOPIC", "PARTITION" })
    // public void shouldNotInstruct() {
    // Builder<String, String> builder = RecordInspector.stringSelectorsBuilder();
    // builder.instruct("name1", "VALUE");
    // builder.instruct("name2", "KEY");
    // builder.instruct("name3", "TIMESTAMP");
    // builder.instruct("name4", "TOPIC");
    // builder.instruct("name5", "PARTITION");
    // }

}
