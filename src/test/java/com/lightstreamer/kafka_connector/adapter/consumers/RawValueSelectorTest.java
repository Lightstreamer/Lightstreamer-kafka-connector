package com.lightstreamer.kafka_connector.adapter.consumers;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lightstreamer.kafka_connector.adapter.consumers.raw.RawValueSelector;

public class RawValueSelectorTest {

    static class Value {

        public String text;

        public Value child;

        public List<Value> children;

        public Value[][] family;

        public Value(String text) {
            this.text = text;
        }

        public Value(String text, List<Value> children) {
            this(text);
            this.children = List.copyOf(children);
        }

        public Value(String value, Value[][] family) {
            this(value);
            this.family = family;
        }
    }

    static RawValueSelector selector(String expression) {
        return new RawValueSelector("name", expression);
    }

    @Test
    public void shouldExtractSimpleExpression() {
        Value value = new Value("joe");
        RawValueSelector rawValueSelector = new RawValueSelector("name", "VALUE.text");
        assertThat(rawValueSelector.name()).isEqualTo("name");
        // assertValue(root, "VALUE.text", "joe");
    }

    @Test
    public void shouldExtractNestedExpression() {
        Value value = new Value("joe",
                List.of(new Value("alex"),
                        new Value("anna",
                                List.of(new Value("gloria"), new Value("terence"))),
                        new Value("serena")));

        ObjectNode node = new ObjectMapper().valueToTree(value);
        // assertValue(node, "VALUE.children[0].text", "alex");
        // assertValue(node, "VALUE.children[1].text", "anna");
        // assertValue(node, "VALUE.children[2].text", "serena");
        // assertValue(node, "VALUE.children[1].children[0].text", "gloria");
        // assertValue(node, "VALUE.children[1].children[1].text", "terence");
    }

    static void assertValue(String value, String expression, String expected) {
        RawValueSelector s = selector(expression);
        assertThat(s.extract(value).text()).isEqualTo(expected);
    }
}
