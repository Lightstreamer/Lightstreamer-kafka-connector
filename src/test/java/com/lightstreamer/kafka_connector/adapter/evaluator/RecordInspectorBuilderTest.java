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

import com.lightstreamer.kafka_connector.adapter.evaluator.RecordInspector.Builder;

public class RecordInspectorBuilderTest {

   static Stream<Builder<?, ?>> builderProvider() {
        return Stream.of(RecordInspector.noSelectorsBuilder(), RecordInspector.stringSelectorsBuilder());
    }

    @Tag("unit")
    @ParameterizedTest
    @MethodSource("builderProvider")
    public void shouldBuildNotInstructedInspector(Builder<?, ?> builder) {
        RecordInspector<?, ?> inspector = builder.build();
        assertThat(inspector.metaSelectors()).isEmpty();
        assertThat(inspector.valueSelectors()).isEmpty();
        assertThat(inspector.keySelectors()).isEmpty();
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
        added = rawBuilder.addKeySelector("name", "KEY");
        assertThat(added).isFalse();
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
        Builder<?, ?> rawBuilder = RecordInspector.noSelectorsBuilder();
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

    // @ParameterizedTest(name = "[{index}] {arguments}")
    // @CsvSource(useHeadersInDisplayName = true, textBlock = """
    // ATTRIBUTE, VALUE
    // TOPIC, record-topic
    // PARTITION, 150
    // """)
    // public void shouldExtractAttribute(String attributeName, String
    // expectedValue) {
    // MetaSelectorImpl r = new MetaSelectorImpl("field_name", attributeName);
    // Value value = r.extract(record());
    // assertThat(value.name()).isEqualTo("field_name");
    // assertThat(value.text()).isEqualTo(expectedValue);
    // }

    // @Test
    // public void shouldNotExtractAttribute() {
    // MetaSelectorImpl r = new MetaSelectorImpl("field_name",
    // "NOT-EXISTING-ATTRIBUTE");
    // Value value = r.extract(record());
    // assertThat(value.name()0).isEqualTo("field_name");
    // assertThat(value.text()).isEqualTo("Not-existing record attribute");
    // }
}
