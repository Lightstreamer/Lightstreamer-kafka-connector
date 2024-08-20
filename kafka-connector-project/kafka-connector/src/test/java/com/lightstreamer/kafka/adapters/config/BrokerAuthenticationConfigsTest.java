
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

package com.lightstreamer.kafka.adapters.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.SASL_MECHANISM;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;

import org.junit.jupiter.api.Test;

public class BrokerAuthenticationConfigsTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = BrokerAuthenticationConfigs.spec();
        ConfParameter salsMechanism =
                configSpec.getParameter(BrokerAuthenticationConfigs.SASL_MECHANISM);
        assertThat(salsMechanism.name()).isEqualTo(BrokerAuthenticationConfigs.SASL_MECHANISM);
        assertThat(salsMechanism.required()).isFalse();
        assertThat(salsMechanism.multiple()).isFalse();
        assertThat(salsMechanism.mutable()).isTrue();
        assertThat(salsMechanism.defaultValue()).isEqualTo("PLAIN");
        assertThat(salsMechanism.type()).isEqualTo(SASL_MECHANISM);

        ConfParameter username = configSpec.getParameter(BrokerAuthenticationConfigs.USERNAME);
        assertThat(username.name()).isEqualTo(BrokerAuthenticationConfigs.USERNAME);
        assertThat(username.required()).isFalse();
        assertThat(username.multiple()).isFalse();
        assertThat(username.mutable()).isTrue();
        assertThat(username.defaultValue()).isNull();
        assertThat(username.type()).isEqualTo(TEXT);

        ConfParameter password = configSpec.getParameter(BrokerAuthenticationConfigs.PASSWORD);
        assertThat(password.name()).isEqualTo(BrokerAuthenticationConfigs.PASSWORD);
        assertThat(password.required()).isFalse();
        assertThat(password.multiple()).isFalse();
        assertThat(password.mutable()).isTrue();
        assertThat(password.defaultValue()).isNull();
        assertThat(password.type()).isEqualTo(TEXT);

        ConfParameter useKeyTab =
                configSpec.getParameter(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE);
        assertThat(useKeyTab.name()).isEqualTo(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_ENABLE);
        assertThat(useKeyTab.required()).isFalse();
        assertThat(useKeyTab.multiple()).isFalse();
        assertThat(useKeyTab.mutable()).isTrue();
        assertThat(useKeyTab.defaultValue()).isEqualTo("false");
        assertThat(useKeyTab.type()).isEqualTo(BOOL);

        ConfParameter storeKey =
                configSpec.getParameter(BrokerAuthenticationConfigs.GSSAPI_STORE_KEY_ENABLE);
        assertThat(storeKey.name()).isEqualTo(BrokerAuthenticationConfigs.GSSAPI_STORE_KEY_ENABLE);
        assertThat(storeKey.required()).isFalse();
        assertThat(storeKey.multiple()).isFalse();
        assertThat(storeKey.mutable()).isTrue();
        assertThat(storeKey.defaultValue()).isEqualTo("false");
        assertThat(storeKey.type()).isEqualTo(BOOL);

        ConfParameter keyTab =
                configSpec.getParameter(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH);
        assertThat(keyTab.name()).isEqualTo(BrokerAuthenticationConfigs.GSSAPI_KEY_TAB_PATH);
        assertThat(keyTab.required()).isFalse();
        assertThat(keyTab.multiple()).isFalse();
        assertThat(keyTab.mutable()).isTrue();
        assertThat(keyTab.defaultValue()).isNull();
        assertThat(keyTab.type()).isEqualTo(FILE);

        ConfParameter principal =
                configSpec.getParameter(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL);
        assertThat(principal.name()).isEqualTo(BrokerAuthenticationConfigs.GSSAPI_PRINCIPAL);
        assertThat(principal.required()).isFalse();
        assertThat(principal.multiple()).isFalse();
        assertThat(principal.mutable()).isTrue();
        assertThat(principal.defaultValue()).isNull();
        assertThat(principal.type()).isEqualTo(TEXT);

        ConfParameter kerberosServiceName =
                configSpec.getParameter(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME);
        assertThat(kerberosServiceName.name())
                .isEqualTo(BrokerAuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME);
        assertThat(kerberosServiceName.required()).isTrue();
        assertThat(kerberosServiceName.multiple()).isFalse();
        assertThat(kerberosServiceName.mutable()).isTrue();
        assertThat(kerberosServiceName.defaultValue()).isNull();
        assertThat(kerberosServiceName.type()).isEqualTo(TEXT);

        ConfParameter useTicketCache =
                configSpec.getParameter(BrokerAuthenticationConfigs.GSSAPI_TICKET_CACHE_ENABLE);
        assertThat(useTicketCache.name())
                .isEqualTo(BrokerAuthenticationConfigs.GSSAPI_TICKET_CACHE_ENABLE);
        assertThat(useTicketCache.required()).isFalse();
        assertThat(useTicketCache.multiple()).isFalse();
        assertThat(useTicketCache.mutable()).isTrue();
        assertThat(useTicketCache.defaultValue()).isEqualTo("false");
        assertThat(useTicketCache.type()).isEqualTo(BOOL);
    }
}
