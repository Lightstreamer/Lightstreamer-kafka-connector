package com.lightstreamer.kafka_connector.adapter.mapping.selectors.string;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithKey;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithStringValue;
import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelector;

@Tag("unit")
public class StringSelectorsTest {

    static ValueSelector<String> valueSelector(String expression) {
        return new StringValueSelectorSupplier().selector("name", expression);
    }

    static KeySelector<String> keySelector(String expression) {
        return new StringKeySelectorSupplier().selector("name", expression);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION, EXPECTED
            VALUE,      joe
            VALUE,      alex
            """)
    public void shouldExtractValue(String expression, String expected) {
        ValueSelector<String> selector = valueSelector(expression);
        assertThat(selector.name()).isEqualTo("name");
        assertThat(selector.extract(recordWithStringValue(expected)).text()).isEqualTo(expected);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION, EXPECTED
            KEY,        joe
            KEY,        alex
            """)
    public void shouldExtractKey(String expression, String expected) {
        KeySelector<String> selector = keySelector(expression);
        assertThat(selector.name()).isEqualTo("name");
        assertThat(selector.extract(recordWithKey(expected)).text()).isEqualTo(expected);
    }

    @Test
    public void shouldNotCreate() {
        ExpressionException e1 = assertThrows(ExpressionException.class, () -> keySelector("invalidKey"));
        assertThat(e1.getMessage()).isEqualTo("Expected <KEY>");

        ExpressionException e2 = assertThrows(ExpressionException.class, () -> keySelector(""));
        assertThat(e2.getMessage()).isEqualTo("Expected <KEY>");

        ExpressionException e3 = assertThrows(ExpressionException.class, () -> valueSelector("invalidValue"));
        assertThat(e3.getMessage()).isEqualTo("Expected <VALUE>");

        ExpressionException e4 = assertThrows(ExpressionException.class, () -> valueSelector(""));
        assertThat(e4.getMessage()).isEqualTo("Expected <VALUE>");

    }
}
