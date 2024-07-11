package com.lightstreamer.kafka.connect.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;

public class RecordErrorHandlingStrategyValidatorTest {

    RecordErrorHandlingStrategyValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new RecordErrorHandlingStrategyValidator();
    }    

    @ParameterizedTest
    @EnumSource
    public void shouldValidate(RecordErrorHandlingStrategy value) {
        assertDoesNotThrow(() -> validator.ensureValid(RECORD_EXTRACTION_ERROR_STRATEGY, value.toString()));
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                    VALUE     | EXPECTED_ERR_MESSAGE
                              | Invalid value for configuration "record.extraction.error.strategy": Must be non-null
                    ''        | Invalid value for configuration "record.extraction.error.strategy": Must be a non-empty string
                    '  '      | Invalid value for configuration "record.extraction.error.strategy": Must be a non-empty string
                    NON_VALID | Invalid value for configuration "record.extraction.error.strategy": Must be a valid strategy
                """)
    public void shouldNotValidate(Object value, String expectedErrorMessage) {
        ConfigException ce =
                assertThrows(
                        ConfigException.class, () -> validator.ensureValid(RECORD_EXTRACTION_ERROR_STRATEGY, value));
        assertThat(ce.getMessage()).isEqualTo(expectedErrorMessage);
    }    
}
