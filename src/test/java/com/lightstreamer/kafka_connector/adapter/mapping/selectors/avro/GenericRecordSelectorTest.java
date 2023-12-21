package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.fromKey;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.fromValue;
import static com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider.RECORD;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;

@Tag("unit")
public class GenericRecordSelectorTest {

    static ValueSelector<GenericRecord> valueSelector(String expression) {
        return GeneircRecordSelectorsSuppliers.valueSelectorSupplier().newSelector("name", expression);
    }

    static KeySelector<GenericRecord> keySelector(String expression) {
        return GeneircRecordSelectorsSuppliers.keySelectorSupplier().newSelector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION,                         EXPECTED
            VALUE.name,                         joe
            VALUE.children[0].name,             alex
            VALUE.children[1].name,             anna
            VALUE.children[2].name,             serena
            VALUE.children[1].children[0].name, gloria
            VALUE.children[1].children[1].name, terence
            """)
    public void shouldExtractValue(String expression, String expectedValue) {
        ValueSelector<GenericRecord> selector = valueSelector(expression);
        assertThat(selector.extract(fromValue(RECORD)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION,                       EXPECTED
            KEY.name,                         joe
            KEY.children[0].name,             alex
            KEY.children[1].name,             anna
            KEY.children[2].name,             serena
            KEY.children[1].children[0].name, gloria
            KEY.children[1].children[1].name, terence
            """)
    public void shouldExtractKey(String expression, String expectedValue) {
        KeySelector<GenericRecord> selector = keySelector(expression);
        assertThat(selector.extract(fromKey(RECORD)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            ESPRESSION,                        EXPECTED_ERROR_MESSAGE
            invalidKey,                        Expected <KEY>
            "",                                Expected <KEY>
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
            "",                                Expected <VALUE>
            VALUE,                             Incomplete expression
            VALUE.,                            Incomplete expression
            VALUE..,                           Tokens cannot be blank
            """)
    public void shouldNotCreateValueSelector(String expression, String expectedErrorMessage) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> valueSelector(expression));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
