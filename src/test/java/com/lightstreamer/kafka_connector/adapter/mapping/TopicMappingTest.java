package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.mapping.TopicMapping;

@Tag("unit")
public class TopicMappingTest {

    @ParameterizedTest
    @MethodSource("wrongProvider")
    public void shouldNotCreate(String topic, List<String> templates, String errorMessage) {
        NullPointerException e = assertThrows(
                NullPointerException.class,
                () -> new TopicMapping(topic, templates));
        assertThat(e.getMessage()).isEqualTo(errorMessage);
    }

    static Stream<Arguments> wrongProvider() {
        return Stream.of(
                Arguments.of(null, Collections.emptyList(), "Null topic"),
                Arguments.of("topic", null, "Null templates"));
    }

    @ParameterizedTest
    @MethodSource("provider")
    public void shouldCreate(String topic, List<String> templates) {
        TopicMapping tm = new TopicMapping(topic, templates);
        assertThat(tm.topic()).isEqualTo(topic);
        assertThat(tm.itemTemplates()).isEqualTo(templates);
    }

    static Stream<Arguments> provider() {
        return Stream.of(
                Arguments.of("topic", Collections.emptyList()),
                Arguments.of("topic", List.of("template1", "template2")));
    }

    // @ParameterizedTest
    // @MethodSource("templatesProvider")
    // public void shouldCreateWithMoreTemplates(List<String> templates) {
    // TopicMapping tm = makeTopic(templates);
    // assertThat(tm.topic()).isEqualTo(TOPIC);
    // assertThat(tm.itemTemplates()).isEqualTo(templates);

    // List<ItemTemplate<String, String>> itemTemplates =
    // tm.createItemTemplates(builder());
    // assertThat(itemTemplates).hasSize(templates.size());
    // itemTemplates.stream().forEach(it -> {
    // assertThat(it.topic()).isEqualTo(TOPIC);
    // assertThat(it.inspector().valueSelectors()).isEmpty();
    // });
    // }

    static Stream<List<String>> templatesProvider() {
        return Stream.of(
                Collections.emptyList(),
                List.of("template1"),
                List.of("template1", "template2"));
    }

    // @ParameterizedTest
    // @MethodSource("topicMappingProvider")
    // public void shouldFlateItemTemplates(List<TopicMapping> topicMappings,
    // List<String> expected) {
    // List<ItemTemplate<String, String>> templates =
    // TopicMapping.flatItemTemplates(topicMappings, builder());
    // assertThat(templates.stream().map(ItemTemplate::prefix).toList())
    // .containsExactly(expected.toArray());

    // }

    // static Stream<Arguments> topicMappingProvider() {
    // return Stream.of(
    // Arguments.of(
    // List.of(
    // new TopicMapping("topic1", List.of("it1"))),
    // List.of("it1")),
    // Arguments.of(
    // List.of(
    // new TopicMapping("topic1", List.of("it1", "it2"))),
    // List.of("it1", "it2")),
    // Arguments.of(
    // List.of(
    // new TopicMapping("topic1", List.of("it1", "it2")),
    // new TopicMapping("topic2", List.of("ita1", "ita2"))),
    // List.of("it1", "it2", "ita1", "ita2")));

    // }
}
