package com.lightstreamer.kafka_connector.adapter.selectors.avro;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithGenericRecordKey;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.recordWithGenericRecordValue;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.KeySelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.avro.GenericRecordKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.avro.GenericRecordValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider;

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
            ${VALUE.name},                         joe
            ${VALUE.children[0].name},             alex
            ${VALUE.children[1].name},             anna
            ${VALUE.children[2].name},             serena
            ${VALUE.children[1].children[0].name}, gloria
            ${VALUE.children[1].children[1].name}, terence
            """)
    public void shouldExtractValue(String expression, String expectedValue) {
        GenericRecord value = GenericRecordProvider.RECORD;
        ValueSelector<GenericRecord> selector = valueSelector(expression);
        assertThat(selector.extract(recordWithGenericRecordValue(value)).text()).isEqualTo(expectedValue);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            EXPRESSION,                       EXPECTED
            ${KEY.name},                         joe
            ${KEY.children[0].name},             alex
            ${KEY.children[1].name},             anna
            ${KEY.children[2].name},             serena
            ${KEY.children[1].children[0].name}, gloria
            ${KEY.children[1].children[1].name}, terence
            """)
    public void shouldExtractKey(String expression, String expectedValue) {
        GenericRecord value = GenericRecordProvider.RECORD;
        KeySelector<GenericRecord> selector = keySelector(expression);
        assertThat(selector.extract(recordWithGenericRecordKey(value)).text()).isEqualTo(expectedValue);
    }
}
