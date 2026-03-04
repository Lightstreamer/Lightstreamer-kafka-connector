
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.connect.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ProxyAdapterAddressValidatorTest {

    ProxyAdapterAddressValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new ProxyAdapterAddressValidator();
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                    VALUE     | EXPECTED_ERR_MESSAGE
                              | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be non-null
                    ''        | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be a non-empty string
                    '  '      | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be a non-empty string
                    host      | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be a valid address in the format <host>:<port>
                    host:     | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be a valid address in the format <host>:<port>
                    host:host | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be a valid address in the format <host>:<port>
                    host:0555 | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be a valid address in the format <host>:<port>
                    -         | Invalid value for configuration "lightstreamer.server.proxy_adapter_address": Must be a valid address in the format <host>:<port>
                """)
    void shouldNotValidate(String value, String expectedErrMessage) {
        assertThrows(
                ConfigException.class,
                () -> validator.ensureValid(LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, value));
    }

    @Test
    void shouldNotValidateDueToNonStringValue() {
        ConfigException ce =
                assertThrows(
                        ConfigException.class,
                        () ->
                                validator.ensureValid(
                                        LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, new Object()));
        assertThat(ce)
                .hasMessageThat()
                .isEqualTo(
                        "Invalid value for configuration \"lightstreamer.server.proxy_adapter.address\": Must be a string");
    }

    @ParameterizedTest
    @ValueSource(strings = {"host:6661", "my-host:66662"})
    void shouldValidate(String value) {
        assertDoesNotThrow(() -> validator.ensureValid(LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, value));
    }
}
