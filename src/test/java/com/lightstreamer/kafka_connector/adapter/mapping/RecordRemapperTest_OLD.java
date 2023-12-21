package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.lightstreamer.kafka_connector.adapter.mapping.RecordRemapper.Builder;
    
public class RecordRemapperTest {

    static Stream<Builder<?, ?>> builderProvider() {
        return Stream.init(RecordRemapper.noSelectorsBuilder(), RecordRemapper.stringSelectorsBuilder());
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("builderProvider")
    public void shouldBuildNotInstructedInspector(Builder<?, ?> builder) {
        RecordRemapper<?, ?> inspector = builder.build();
        assertThat(inspector.names()).isEmpty();
    }

    @Test
    public void shouldNotInstructInspector() {
        Builder<?, ?> builder = RecordRemapper.noSelectorsBuilder();
        Exception exception = assertThrows(RuntimeException.class, () -> builder.instruct("name", "unknown"));
        assertThat(exception.getMessage()).isEqualTo("Unknown expression <unknown>");
    }

    @Test
    public void shouldAddKeySelector() {
        Builder<String, String> rawBuilder = RecordRemapper.stringSelectorsBuilder();
        boolean added = rawBuilder.addKeySelector("name", "KEY");
        assertThat(added).isTrue();

        // No duplicates
        assertThrows(RuntimeException.class, () -> rawBuilder.addKeySelector("name", "KEY"));
        assertThrows(RuntimeException.class, () -> rawBuilder.addValueSelector("name", "VALUE"));
        assertThrows(RuntimeException.class, () -> rawBuilder.addMetaSelector("name", "TOPIC"));
    }

    @Test
    public void shouldNotAddKeySelector() {
        Builder<?, ?> rawBuilder = RecordRemapper.noSelectorsBuilder();
        boolean added = rawBuilder.addKeySelector("name", "KEY");
        assertThat(added).isFalse();

        rawBuilder.addKeySelector("name", "WRONG_EXPRESSION");
        assertThat(added).isFalse();
    }

    @Test
    public void shouldAddValueSelector() {
        Builder<String, String> rawBuilder = RecordRemapper.stringSelectorsBuilder();
        boolean added = rawBuilder.addValueSelector("name", "VALUE");
        assertThat(added).isTrue();

        // No duplicates
        added = rawBuilder.addValueSelector("name", "VALUE");
        assertThat(added).isFalse();
    }

    @Test
    public void shouldNotAddValueSelector() {
        Builder<?, ?> rawBuilder = RecordRemapper.stringSelectorsBuilder();
        boolean added = rawBuilder.addValueSelector("name", "VALUE");
        assertThat(added).isFalse();

        // No wrong expressions
        rawBuilder.addValueSelector("name", "WRONG_EXPRESSION");
        assertThat(added).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "TIMESTAMP", "TOPIC", "PARTITION" })
    public void shouldAddMetaSelector(String expression) {
        Builder<?, ?> builder1 = RecordRemapper.noSelectorsBuilder();
        Builder<?, ?> builder2 = RecordRemapper.stringSelectorsBuilder();

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
        Builder<String, String> builder = RecordRemapper.builder();
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
