package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithKey;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithValue;
import static com.lightstreamer.kafka_connector.adapter.test_utils.JsonNodeProvider.RECORD;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;

@Tag("unit")
public class JsonNodeSelectorSuppliersTest {

    static ValueSelector<JsonNode> valueSelector(String expression) {
        return new JsonNodeValueSelectorSupplier().selector("name", expression);
    }

    static KeySelector<JsonNode> keySelector(String expression) {
        return new JsonNodeKeySelectorSupplier().selector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION,                         EXPECTED_VALUE
            VALUE.name,                         joe
            VALUE.children[0].name,             alex
            VALUE.children[1].name,             anna
            VALUE.children[2].name,             serena
            VALUE.children[1].children[0].name, gloria
            VALUE.children[1].children[1].name, terence
            """)
    public void shouldExtractValue(String expression, String expectedValue) {
        ValueSelector<JsonNode> s = valueSelector(expression);
        assertThat(s.extract(recordWithValue(RECORD)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            ESPRESSION,                        EXPECTED_VALUE
            KEY.name,                          joe
            KEY.children[0].name,              alex
            KEY.children[1].name,              anna
            KEY.children[2].name,              serena
            KEY.children[1].children[0].name,  gloria
            KEY.children[1].children[1].name,  terence
            """)
    public void shouldExtractKey(String expression, String expectedValue) {
        KeySelector<JsonNode> s = keySelector(expression);
        assertThat(s.extract(recordWithKey(RECORD)).text()).isEqualTo(expectedValue);
    }
}
