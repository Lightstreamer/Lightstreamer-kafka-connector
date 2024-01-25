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
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
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
            VALUE.children[0],                  The expression [VALUE.children[0]] must evaluate to a non-complex object
            """)
    public void shouldNotExtractValue(String expression, String errorMessage) {
        ValueSelector<JsonNode> selector = valueSelector(expression);
        ValueException ve = assertThrows(ValueException.class, () ->selector.extract(fromValue(RECORD)).text());
        assertThat(ve.getMessage()).isEqualTo(errorMessage);
    }

    @Test
    public void shouldExtract() {
        ValueSelector<JsonNode> selector = valueSelector("VALUE.children[0]");
        // ValueException ve = assertThrows(ValueException.class, () ->selector.extract(fromValue(RECORD)).text());
        // assertThat(ve.getMessage()).isEqualTo(errorMessage);
        Value value = selector.extract(fromValue(RECORD));
        System.out.println(value);
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
            '',                                Expected the root token [KEY] while evaluating [name]
            invalidKey,                        Expected the root token [KEY] while evaluating [name]
            KEY,                               Found the invalid expression [KEY] while evaluating [name]
            KEY.,                              Found the invalid expression [KEY.] while evaluating [name]
            KEY..,                             Found the invalid expression [KEY..] with missing tokens while evaluating [name]
            KEY.attrib[],                      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
            KEY.attrib[0]xsd,                  Found the invalid indexed expression [KEY.attrib[0]xsd] while evaluating [name]
            KEY.attrib[],                      Found the invalid indexed expression [KEY.attrib[]] while evaluating [name]
            KEY.attrib[a],                     Found the invalid indexed expression [KEY.attrib[a]] while evaluating [name]
            KEY.attrib[a].,                    Found the invalid indexed expression [KEY.attrib[a].] while evaluating [name]
            KEY.attrib[0].,                    Found the invalid indexed expression [KEY.attrib[0].] while evaluating [name]
            """)
    public void shouldNotCreateKeySelector(String expression, String expectedErrorMessage) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> keySelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            ESPRESSION,                        EXPECTED_ERROR_MESSAGE
            '',                                Expected the root token [VALUE] while evaluating [name]
            invalidValue,                      Expected the root token [VALUE] while evaluating [name]
            VALUE,                             Found the invalid expression [VALUE] while evaluating [name]
            VALUE.,                            Found the invalid expression [VALUE.] while evaluating [name]
            VALUE..,                           Found the invalid expression [VALUE..] with missing tokens while evaluating [name]
            VALUE.attrib[],                    Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
            VALUE.attrib[0]xsd,                Found the invalid indexed expression [VALUE.attrib[0]xsd] while evaluating [name]
            VALUE.attrib[],                    Found the invalid indexed expression [VALUE.attrib[]] while evaluating [name]
            VALUE.attrib[a],                   Found the invalid indexed expression [VALUE.attrib[a]] while evaluating [name]
            VALUE.attrib[a].,                  Found the invalid indexed expression [VALUE.attrib[a].] while evaluating [name]
            """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
