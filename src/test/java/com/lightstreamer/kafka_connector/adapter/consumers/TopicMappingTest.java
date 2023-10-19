package com.lightstreamer.kafka_connector.adapter.consumers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.lightstreamer.kafka_connector.adapter.evaluator.ItemTemplate;
import com.lightstreamer.kafka_connector.adapter.test_utils.IdentityValueSelector;

public class TopicMappingTest {

    private static final String TOPIC = "topic";

    private TopicMapping makeTopic(List<String> templates) {
        return new TopicMapping(TOPIC, templates);
    }

    @ParameterizedTest
    @MethodSource("wrongProvider")
    public void shouldNotCreate(String topic, List<String> templates, String errorMessage) {
        NullPointerException e = assertThrows(NullPointerException.class, () -> new TopicMapping(topic, templates));
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

        List<ItemTemplate<String>> itemTemplates = tm.createItemTemplates(IdentityValueSelector::new);
        assertThat(itemTemplates).hasSize(templates.size());
        itemTemplates.stream().forEach(it -> {
            assertThat(it.topic()).isEqualTo(TOPIC);
            assertThat(it.valueSelectors()).isEmpty();
        });
    }

    static Stream<Arguments> templatesProvider() {
        return Stream.of(
                Arguments.of(Collections.emptyList()),
                Arguments.of(List.of("template1")),
                Arguments.of(List.of("template1", "template2")));
    }

}
