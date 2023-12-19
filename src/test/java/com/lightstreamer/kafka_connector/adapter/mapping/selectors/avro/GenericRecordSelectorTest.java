package com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithKey;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithValue;
import static com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider.RECORD;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;

@Tag("unit")
public class GenericRecordSelectorTest {

    static ValueSelector<GenericRecord> valueSelector(String expression) {
        return new GenericRecordValueSelectorSupplier().selector("name", expression);
    }

    static KeySelector<GenericRecord> keySelector(String expression) {
        return new GenericRecordKeySelectorSupplier().selector("name", expression);
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
        assertThat(selector.extract(recordWithValue(RECORD)).text()).isEqualTo(expectedValue);
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
        assertThat(selector.extract(recordWithKey(RECORD)).text()).isEqualTo(expectedValue);
    }

    @Test
    public void shouldNotCreate() {
        ExpressionException e1 = assertThrows(ExpressionException.class, () -> keySelector("invalidKey"));
        assertThat(e1.getMessage()).isEqualTo("Expected <KEY>");

        ExpressionException e2 = assertThrows(ExpressionException.class, () -> valueSelector("invalidValue"));
        assertThat(e2.getMessage()).isEqualTo("Expected <VALUE>");
    }
}
