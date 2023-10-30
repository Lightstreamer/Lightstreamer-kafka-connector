package com.lightstreamer.kafka_connector.adapter.consumers;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lightstreamer.kafka_connector.adapter.consumers.json.JsonNodeSelector;
import static com.google.common.truth.Truth.assertThat;

public class JsonNodeSelectorTest {

    static class Value {

        public String text;

        public String[] info;

        public Value child;

        public Value[] sibillings;

        public Value[][] family;

        public Value(String text) {
            this.text = text;
        }

        public Value(Value child) {
            this.child = child;
        }

        public Value(Value[] sibillings) {
            this.sibillings = sibillings;
        }

        public Value(Value[] sibillings, String[] info) {
            this.sibillings = sibillings;
            this.info = info;
        }

        public Value(String value, Value[] sibillings) {
            this(value);
            this.sibillings = sibillings;
        }

        public Value(String value, Value[][] family) {
            this(value);
            this.family = family;
        }
    }

    @Test
    public void shouldExtractSimpleExpression() {
        ObjectNode root = new ObjectMapper().valueToTree(new Value("value1"));
        JsonNodeSelector jsonNodeSelector = new JsonNodeSelector("name", "VALUE.text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("value1");
    }

    @Test
    public void shouldExtractComplexExpression() {
        Value v = new Value("nested");
        ObjectNode root = new ObjectMapper().valueToTree(new Value(v));
        JsonNodeSelector jsonNodeSelector = new JsonNodeSelector("name", "VALUE.child.text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("nested");
    }

    @Test
    public void shouldExtractIndexedExpression() {
        Value v1 = new Value("v1");
        Value v2 = new Value("v2", new Value[] { new Value("v1a"), new Value("v1b") });

        Value v3 = new Value("v3");
        Value v = new Value(new Value[] { v1, v2, v3 }, new String[] { "aa", "bb", "cc" });

        ObjectNode root = new ObjectMapper().valueToTree(v);
        JsonNodeSelector jsonNodeSelector = new JsonNodeSelector("name", "VALUE.sibillings[0].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v1");

        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.sibillings[1].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v2");
        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.sibillings[1].sibillings[0].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v1a");
        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.sibillings[1].sibillings[1].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v1b");

        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.sibillings[2].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v3");

        // jsonNodeSelector = new JsonNodeSelector("name", "info[*]");
        // assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v3");
    }

    @Test
    public void shouldExtractComplexIndexedExpression() {
        Value[][] values = new Value[][] {
                new Value[] { new Value("v1a"), new Value("v1b"), new Value("v1c") },
                new Value[] { new Value("v2a"), new Value("v2b"), new Value("v2c") }
        };
        Value v = new Value("text", values);

        ObjectNode root = new ObjectMapper().valueToTree(v);
        JsonNodeSelector jsonNodeSelector = new JsonNodeSelector("name", "VALUE.family[0][0].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v1a");

        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.family[0][1].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v1b");

        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.family[0][2].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v1c");

        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.family[1][0].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v2a");

        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.family[1][1].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v2b");

        jsonNodeSelector = new JsonNodeSelector("name", "VALUE.family[1][2].text");
        assertThat(jsonNodeSelector.extract(root).text()).isEqualTo("v2c");
    }
}
