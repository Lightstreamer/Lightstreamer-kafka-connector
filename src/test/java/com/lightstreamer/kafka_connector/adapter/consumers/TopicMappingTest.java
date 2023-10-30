package com.lightstreamer.kafka_connector.adapter.consumers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.evaluator.IdentityValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.ItemTemplate;
import com.lightstreamer.kafka_connector.adapter.evaluator.RecordInspector;

public class TopicMappingTest {

    private static final String TOPIC = "topic";

    private TopicMapping makeTopic(List<String> templates) {
        return new TopicMapping(TOPIC, templates);
    }

    private RecordInspector.Builder<String, String> builder() {
        return new RecordInspector.Builder<>(
                IdentityValueSelector::new, IdentityValueSelector::new);
    }

    @ParameterizedTest
    @MethodSource("wrongProvider")
    public void shouldNotCreate(String topic, List<String> templates, String errorMessage) {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> new TopicMapping(topic, templates));
        assertThat(e.getMessage()).isEqualTo(errorMessage);
    }

    static Stream<Arguments> wrongProvider() {
        return Stream.of(
                Arguments.of(null, Collections.emptyList(), "Null topic"),
                Arguments.of("topic", null, "Null templates"));
    }

    @ParameterizedTest
    @MethodSource("templatesProvider")
    public void shouldCreateWithMoreTemplates(List<String> templates) {
        TopicMapping tm = makeTopic(templates);
        assertThat(tm.topic()).isEqualTo(TOPIC);
        assertThat(tm.itemTemplates()).isEqualTo(templates);

        List<ItemTemplate<String, String>> itemTemplates = tm.createItemTemplates(builder());
        assertThat(itemTemplates).hasSize(templates.size());
        itemTemplates.stream().forEach(it -> {
            assertThat(it.topic()).isEqualTo(TOPIC);
            assertThat(it.inspector().valueSelectors()).isEmpty();
        });
    }

    static Stream<Arguments> templatesProvider() {
        return Stream.of(
                Arguments.of(Collections.emptyList()),
                Arguments.of(List.of("template1")),
                Arguments.of(List.of("template1", "template2")));
    }

    @ParameterizedTest
    @MethodSource("topicMappingProvider")
    public void shouldFlateItemTemplates(List<TopicMapping> topicMappings, List<String> expected) {
        List<ItemTemplate<String, String>> templates = TopicMapping.flatItemTemplates(topicMappings, builder());
        assertThat(templates.stream().map(ItemTemplate::prefix).toList())
                .containsExactly(expected.toArray());

    }

    static Stream<Arguments> topicMappingProvider() {
        return Stream.of(
                Arguments.of(
                        List.of(
                                new TopicMapping("topic1", List.of("it1"))),
                        List.of("it1")),
                Arguments.of(
                        List.of(
                                new TopicMapping("topic1", List.of("it1", "it2"))),
                        List.of("it1", "it2")),
                Arguments.of(
                        List.of(
                                new TopicMapping("topic1", List.of("it1", "it2")),
                                new TopicMapping("topic2", List.of("ita1", "ita2"))),
                        List.of("it1", "it2", "ita1", "ita2")));

    }
}
