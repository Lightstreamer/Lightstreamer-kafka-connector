package com.lightstreamer.kafka_connector.adapter.selectors.json;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.json.JsonNodeKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.json.JsonNodeValueSelectorSupplier;

@Tag("unit")
public class JsonNodeSelectorSuppliersTest {

    static class Value {

        public String name;

        public Value child;

        public List<Value> children;

        public Value[][] family;

        public Value(String name) {
            this.name = name;
        }

        public Value(String name, List<Value> children) {
            this(name);
            this.children = List.copyOf(children);
        }

        public Value(String name, Value[][] family) {
            this(name);
            this.family = family;
        }
    }

    private static ConsumerRecord<JsonNode, ?> recordWithKey(JsonNode key) {
        return record(key, null);
    }

    private static ConsumerRecord<?, JsonNode> recordWithValue(JsonNode value) {
        return record(null, value);
    }

    private static ConsumerRecord<JsonNode, JsonNode> record(JsonNode key, JsonNode value) {
        return new ConsumerRecord<>(
                "record-topic",
                150,
                120,
                ConsumerRecord.NO_TIMESTAMP,
                TimestampType.NO_TIMESTAMP_TYPE,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                key,
                value,
                new RecordHeaders(),
                Optional.empty());
    }

    static ValueSelector<JsonNode> valueSelector(String expression) {
        return new JsonNodeValueSelectorSupplier().selector("name", expression);
    }

    static KeySelector<JsonNode> keySelector(String expression) {
        return new JsonNodeKeySelectorSupplier().selector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION,                            EXPECTED_VALUE
            ${VALUE.name},                         joe
            ${VALUE.children[0].name},             alex
            ${VALUE.children[1].name},             anna
            ${VALUE.children[2].name},             serena
            ${VALUE.children[1].children[0].name}, gloria
            ${VALUE.children[1].children[1].name}, terence
            """)
    public void shouldExtractValue(String expression, String expectedValue) {
        ObjectNode node = newNode();
        ValueSelector<JsonNode> s = valueSelector(expression);
        assertThat(s.extract(recordWithValue(node)).text()).isEqualTo(expectedValue);
    }

    private ObjectNode newNode() {
        Value value = new Value("joe",
                List.of(new Value("alex"),
                        new Value("anna",
                                List.of(new Value("gloria"), new Value("terence"))),
                        new Value("serena")));

        ObjectNode node = new ObjectMapper().valueToTree(value);
        return node;
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            ESPRESSION,                           EXPECTED_VALUE
            ${KEY.name},                          joe
            ${KEY.children[0].name},              alex
            ${KEY.children[1].name},              anna
            ${KEY.children[2].name},              serena
            ${KEY.children[1].children[0].name},  gloria
            ${KEY.children[1].children[1].name},  terence
            """)
    public void shouldExtractKey(String expression, String expectedValue) {
        ObjectNode node = newNode();
        KeySelector<JsonNode> s = keySelector(expression);
        assertThat(s.extract(recordWithKey(node)).text()).isEqualTo(expectedValue);
    }
}
