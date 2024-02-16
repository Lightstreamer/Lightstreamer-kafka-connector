
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

package com.lightstreamer.kafka_connector.adapters.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.SASL_MECHANISM;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfParameter;

import org.junit.jupiter.api.Test;

public class AuthenticationConfigsTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = AuthenticationConfigs.spec();

        ConfParameter salsMechanism = configSpec.getParameter(AuthenticationConfigs.SASL_MECHANISM);
        assertThat(salsMechanism.name()).isEqualTo(AuthenticationConfigs.SASL_MECHANISM);
        assertThat(salsMechanism.required()).isFalse();
        assertThat(salsMechanism.multiple()).isFalse();
        assertThat(salsMechanism.mutable()).isTrue();
        assertThat(salsMechanism.defaultValue()).isEqualTo("PLAIN");
        assertThat(salsMechanism.type()).isEqualTo(SASL_MECHANISM);

        ConfParameter username = configSpec.getParameter(AuthenticationConfigs.USERNAME);
        assertThat(username.name()).isEqualTo(AuthenticationConfigs.USERNAME);
        assertThat(username.required()).isFalse();
        assertThat(username.multiple()).isFalse();
        assertThat(username.mutable()).isTrue();
        assertThat(username.defaultValue()).isNull();
        assertThat(username.type()).isEqualTo(TEXT);

        ConfParameter password = configSpec.getParameter(AuthenticationConfigs.PASSWORD);
        assertThat(password.name()).isEqualTo(AuthenticationConfigs.PASSWORD);
        assertThat(password.required()).isFalse();
        assertThat(password.multiple()).isFalse();
        assertThat(password.mutable()).isTrue();
        assertThat(password.defaultValue()).isNull();
        assertThat(password.type()).isEqualTo(TEXT);

        ConfParameter useKeyTab = configSpec.getParameter(AuthenticationConfigs.GSSAPI_USE_KEY_TAB);
        assertThat(useKeyTab.name()).isEqualTo(AuthenticationConfigs.GSSAPI_USE_KEY_TAB);
        assertThat(useKeyTab.required()).isFalse();
        assertThat(useKeyTab.multiple()).isFalse();
        assertThat(useKeyTab.mutable()).isTrue();
        assertThat(useKeyTab.defaultValue()).isEqualTo("false");
        assertThat(useKeyTab.type()).isEqualTo(BOOL);

        ConfParameter storeKey = configSpec.getParameter(AuthenticationConfigs.GSSAPI_STORE_KEY);
        assertThat(storeKey.name()).isEqualTo(AuthenticationConfigs.GSSAPI_STORE_KEY);
        assertThat(storeKey.required()).isFalse();
        assertThat(storeKey.multiple()).isFalse();
        assertThat(storeKey.mutable()).isTrue();
        assertThat(storeKey.defaultValue()).isEqualTo("false");
        assertThat(storeKey.type()).isEqualTo(BOOL);

        ConfParameter keyTab = configSpec.getParameter(AuthenticationConfigs.GSSAPI_KEY_TAB);
        assertThat(keyTab.name()).isEqualTo(AuthenticationConfigs.GSSAPI_KEY_TAB);
        assertThat(keyTab.required()).isFalse();
        assertThat(keyTab.multiple()).isFalse();
        assertThat(keyTab.mutable()).isTrue();
        assertThat(keyTab.defaultValue()).isNull();
        assertThat(keyTab.type()).isEqualTo(FILE);

        ConfParameter principal = configSpec.getParameter(AuthenticationConfigs.GSSAPI_PRINCIPAL);
        assertThat(principal.name()).isEqualTo(AuthenticationConfigs.GSSAPI_PRINCIPAL);
        assertThat(principal.required()).isTrue();
        assertThat(principal.multiple()).isFalse();
        assertThat(principal.mutable()).isTrue();
        assertThat(principal.defaultValue()).isNull();
        assertThat(principal.type()).isEqualTo(TEXT);

        ConfParameter kerberosServiceName =
                configSpec.getParameter(AuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME);
        assertThat(kerberosServiceName.name())
                .isEqualTo(AuthenticationConfigs.GSSAPI_KERBEROS_SERVICE_NAME);
        assertThat(kerberosServiceName.required()).isTrue();
        assertThat(kerberosServiceName.multiple()).isFalse();
        assertThat(kerberosServiceName.mutable()).isTrue();
        assertThat(kerberosServiceName.defaultValue()).isNull();
        assertThat(kerberosServiceName.type()).isEqualTo(TEXT);
    }
}
