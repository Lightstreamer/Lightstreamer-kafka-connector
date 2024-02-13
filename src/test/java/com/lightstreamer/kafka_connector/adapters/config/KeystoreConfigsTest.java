
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

import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;

import org.junit.jupiter.api.Test;

public class KeystoreConfigsTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigSpec configSpec = KeystoreConfigs.configSpec();

        ConfParameter keyStoreType = configSpec.getParameter(KeystoreConfigs.KEYSTORE_TYPE);
        assertThat(keyStoreType.name()).isEqualTo(KeystoreConfigs.KEYSTORE_TYPE);
        assertThat(keyStoreType.required()).isFalse();
        assertThat(keyStoreType.multiple()).isFalse();
        assertThat(keyStoreType.mutable()).isTrue();
        assertThat(keyStoreType.defaultValue()).isEqualTo("JKS");
        assertThat(keyStoreType.type()).isEqualTo(ConfType.KEYSTORE_TYPE);

        ConfParameter keystorePath = configSpec.getParameter(KeystoreConfigs.KEYSTORE_PATH);
        assertThat(keystorePath.name()).isEqualTo(KeystoreConfigs.KEYSTORE_PATH);
        assertThat(keystorePath.required()).isTrue();
        assertThat(keystorePath.multiple()).isFalse();
        assertThat(keystorePath.mutable()).isTrue();
        assertThat(keystorePath.defaultValue()).isNull();
        assertThat(keystorePath.type()).isEqualTo(ConfType.FILE);

        ConfParameter keystorePassword = configSpec.getParameter(KeystoreConfigs.KEYSTORE_PASSWORD);
        assertThat(keystorePassword.name()).isEqualTo(KeystoreConfigs.KEYSTORE_PASSWORD);
        assertThat(keystorePassword.required()).isTrue();
        assertThat(keystorePassword.multiple()).isFalse();
        assertThat(keystorePassword.mutable()).isTrue();
        assertThat(keystorePassword.defaultValue()).isNull();
        assertThat(keystorePassword.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyPassword = configSpec.getParameter(KeystoreConfigs.KEY_PASSWORD);
        assertThat(keyPassword.name()).isEqualTo(KeystoreConfigs.KEY_PASSWORD);
        assertThat(keyPassword.required()).isFalse();
        assertThat(keyPassword.multiple()).isFalse();
        assertThat(keyPassword.mutable()).isTrue();
        assertThat(keyPassword.defaultValue()).isNull();
        assertThat(keyPassword.type()).isEqualTo(ConfType.TEXT);
    }
}
