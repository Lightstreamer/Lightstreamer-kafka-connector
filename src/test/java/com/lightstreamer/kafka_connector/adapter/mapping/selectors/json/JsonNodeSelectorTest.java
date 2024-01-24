package com.lightstreamer.kafka_connector.adapter.mapping.selectors.json;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.fromKey;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.fromValue;
import static com.lightstreamer.kafka_connector.adapter.test_utils.JsonNodeProvider.RECORD;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;

@Tag("unit")
public class JsonNodeSelectorTest {

    static ConnectorConfig config = ConnectorConfigProvider.minimal();

    static ValueSelector<JsonNode> valueSelector(String expression) {
        return JsonNodeSelectorsSuppliers.valueSelectorSupplier(config).newSelector("name", expression);
    }

    static KeySelector<JsonNode> keySelector(String expression) {
        return JsonNodeSelectorsSuppliers.keySelectorSupplier(config).newSelector("name", expression);
    }

    @Test
    public void shouldGetDeserializer() {
        Deserializer<JsonNode> keyDeserializer = JsonNodeSelectorsSuppliers.keySelectorSupplier(config).deseralizer();
        assertThat(keyDeserializer).isInstanceOf(JsonNodeDeserializer.class);
        assertThat(JsonNodeDeserializer.class.cast(keyDeserializer).isKey()).isTrue();

        Deserializer<JsonNode> valueDeserializer = JsonNodeSelectorsSuppliers.valueSelectorSupplier(config).deseralizer();
        assertThat(valueDeserializer).isInstanceOf(JsonNodeDeserializer.class);
        assertThat(JsonNodeDeserializer.class.cast(valueDeserializer).isKey()).isFalse();
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
        ValueSelector<JsonNode> selector = valueSelector(expression);
        assertThat(selector.extract(fromValue(RECORD)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION,                         EXPECTED_ERROR_MESSAGE
            VALUE.no_attrib,                    Field [no_attrib] not found
            VALUE.children[0].no_attrib,        Field [no_attrib] not found    
            VALUE.no_children[0],               Field [no_children] not found
            VALUE.name[0],                      Current field is not indexed
            """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueSelector<JsonNode> selector = valueSelector(expression);
        ValueException ve = assertThrows(ValueException.class, () ->selector.extract(fromValue(RECORD)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
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
        KeySelector<JsonNode> selector = keySelector(expression);
        assertThat(selector.extract(fromKey(RECORD)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION,                       EXPECTED_ERROR_MESSAGE
            KEY.no_attrib,                    Field [no_attrib] not found
            KEY.children[0].no_attrib,        Field [no_attrib] not found    
            KEY.no_children[0],               Field [no_children] not found
            KEY.name[0],                      Current field is not indexed
            """)
    public void shouldNotExtractKey(String expression, String errorMessage) {
        KeySelector<JsonNode> selector = keySelector(expression);
        ValueException ve = assertThrows(ValueException.class, () ->selector.extract(fromKey(RECORD)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            ESPRESSION,                        EXPECTED_ERROR_MESSAGE
            invalidKey,                        Expected <KEY>
            '',                                Expected <KEY>
            KEY,                               Incomplete expression
            KEY.,                              Incomplete expression
            """)
    public void shouldNotCreateKeySelector(String expression, String expectedErrorMessage) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            ESPRESSION,                        EXPECTED_ERROR_MESSAGE
            invalidValue,                      Expected <VALUE>
            '',                                Expected <VALUE>
            VALUE,                             Incomplete expression
            VALUE.,                            Incomplete expression
            VALUE..,                           Tokens cannot be blank
            """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
