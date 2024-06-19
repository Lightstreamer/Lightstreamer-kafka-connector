
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

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;

import org.junit.jupiter.api.Test;

public class EncryptionConfigsTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = EncryptionConfigs.spec();

        ConfParameter sslProtocol = configSpec.getParameter(EncryptionConfigs.SSL_PROTOCOL);
        assertThat(sslProtocol.name()).isEqualTo(EncryptionConfigs.SSL_PROTOCOL);
        assertThat(sslProtocol.required()).isFalse();
        assertThat(sslProtocol.multiple()).isFalse();
        assertThat(sslProtocol.mutable()).isTrue();
        assertThat(sslProtocol.defaultValue()).isEqualTo("TLSv1.3");
        assertThat(sslProtocol.type()).isEqualTo(ConfType.SSL_PROTOCOL);

        ConfParameter trustStoreType = configSpec.getParameter(EncryptionConfigs.TRUSTSTORE_TYPE);
        assertThat(trustStoreType.name()).isEqualTo(EncryptionConfigs.TRUSTSTORE_TYPE);
        assertThat(trustStoreType.required()).isFalse();
        assertThat(trustStoreType.multiple()).isFalse();
        assertThat(trustStoreType.mutable()).isTrue();
        assertThat(trustStoreType.defaultValue()).isEqualTo("JKS");
        assertThat(trustStoreType.type()).isEqualTo(ConfType.KEYSTORE_TYPE);

        ConfParameter trustStorePath = configSpec.getParameter(EncryptionConfigs.TRUSTSTORE_PATH);
        assertThat(trustStorePath.name()).isEqualTo(EncryptionConfigs.TRUSTSTORE_PATH);
        assertThat(trustStorePath.required()).isFalse();
        assertThat(trustStorePath.multiple()).isFalse();
        assertThat(trustStorePath.mutable()).isTrue();
        assertThat(trustStorePath.defaultValue()).isNull();
        assertThat(trustStorePath.type()).isEqualTo(ConfType.FILE);

        ConfParameter trustStorePassword =
                configSpec.getParameter(EncryptionConfigs.TRUSTSTORE_PASSWORD);
        assertThat(trustStorePassword.name()).isEqualTo(EncryptionConfigs.TRUSTSTORE_PASSWORD);
        assertThat(trustStorePassword.required()).isFalse();
        assertThat(trustStorePassword.multiple()).isFalse();
        assertThat(trustStorePassword.mutable()).isTrue();
        assertThat(trustStorePassword.defaultValue()).isNull();
        assertThat(trustStorePassword.type()).isEqualTo(ConfType.TEXT);

        ConfParameter enableHostNameVerification =
                configSpec.getParameter(EncryptionConfigs.ENABLE_HOSTNAME_VERIFICATION);
        assertThat(enableHostNameVerification.name())
                .isEqualTo(EncryptionConfigs.ENABLE_HOSTNAME_VERIFICATION);
        assertThat(enableHostNameVerification.required()).isFalse();
        assertThat(enableHostNameVerification.multiple()).isFalse();
        assertThat(enableHostNameVerification.mutable()).isTrue();
        assertThat(enableHostNameVerification.defaultValue()).isEqualTo("false");
        assertThat(enableHostNameVerification.type()).isEqualTo(ConfType.BOOL);

        ConfParameter sslCipherSuites =
                configSpec.getParameter(EncryptionConfigs.SSL_CIPHER_SUITES);
        assertThat(sslCipherSuites.name()).isEqualTo(EncryptionConfigs.SSL_CIPHER_SUITES);
        assertThat(sslCipherSuites.required()).isFalse();
        assertThat(sslCipherSuites.multiple()).isFalse();
        assertThat(sslCipherSuites.mutable()).isTrue();
        assertThat(sslCipherSuites.defaultValue()).isNull();
        assertThat(sslCipherSuites.type()).isEqualTo(ConfType.TEXT_LIST);

        ConfParameter sslProviders = configSpec.getParameter(EncryptionConfigs.SSL_PROVIDER);
        assertThat(sslProviders.name()).isEqualTo(EncryptionConfigs.SSL_PROVIDER);
        assertThat(sslProviders.required()).isFalse();
        assertThat(sslProviders.multiple()).isFalse();
        assertThat(sslProviders.mutable()).isTrue();
        assertThat(sslProviders.defaultValue()).isNull();
        assertThat(sslProviders.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslEngineFactoryClass =
                configSpec.getParameter(EncryptionConfigs.SSL_EGINE_FACTORY_CLASS);
        assertThat(sslEngineFactoryClass.name())
                .isEqualTo(EncryptionConfigs.SSL_EGINE_FACTORY_CLASS);
        assertThat(sslEngineFactoryClass.required()).isFalse();
        assertThat(sslEngineFactoryClass.multiple()).isFalse();
        assertThat(sslEngineFactoryClass.mutable()).isTrue();
        assertThat(sslEngineFactoryClass.defaultValue()).isNull();
        assertThat(sslEngineFactoryClass.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslKeyManagerAlogorithm =
                configSpec.getParameter(EncryptionConfigs.SSL_KEYMANAGER_ALGORITHM);
        assertThat(sslKeyManagerAlogorithm.name())
                .isEqualTo(EncryptionConfigs.SSL_KEYMANAGER_ALGORITHM);
        assertThat(sslKeyManagerAlogorithm.required()).isFalse();
        assertThat(sslKeyManagerAlogorithm.multiple()).isFalse();
        assertThat(sslKeyManagerAlogorithm.mutable()).isTrue();
        assertThat(sslKeyManagerAlogorithm.defaultValue()).isNull();
        assertThat(sslKeyManagerAlogorithm.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslSecureRandomImplementation =
                configSpec.getParameter(EncryptionConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION);
        assertThat(sslSecureRandomImplementation.name())
                .isEqualTo(EncryptionConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION);
        assertThat(sslSecureRandomImplementation.required()).isFalse();
        assertThat(sslSecureRandomImplementation.multiple()).isFalse();
        assertThat(sslSecureRandomImplementation.mutable()).isTrue();
        assertThat(sslSecureRandomImplementation.defaultValue()).isNull();
        assertThat(sslSecureRandomImplementation.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslTrustManagerAlgorithm =
                configSpec.getParameter(EncryptionConfigs.SSL_TRUSTMANAGER_ALGORITHM);
        assertThat(sslTrustManagerAlgorithm.name())
                .isEqualTo(EncryptionConfigs.SSL_TRUSTMANAGER_ALGORITHM);
        assertThat(sslTrustManagerAlgorithm.required()).isFalse();
        assertThat(sslTrustManagerAlgorithm.multiple()).isFalse();
        assertThat(sslTrustManagerAlgorithm.mutable()).isTrue();
        assertThat(sslTrustManagerAlgorithm.defaultValue()).isNull();
        assertThat(sslTrustManagerAlgorithm.type()).isEqualTo(ConfType.TEXT);

        ConfParameter securityProviders =
                configSpec.getParameter(EncryptionConfigs.SECURITY_PROVIDERS);
        assertThat(securityProviders.name()).isEqualTo(EncryptionConfigs.SECURITY_PROVIDERS);
        assertThat(securityProviders.required()).isFalse();
        assertThat(securityProviders.multiple()).isFalse();
        assertThat(securityProviders.mutable()).isTrue();
        assertThat(securityProviders.defaultValue()).isNull();
        assertThat(securityProviders.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyStoreType = configSpec.getParameter(EncryptionConfigs.KEYSTORE_TYPE);
        assertThat(keyStoreType.name()).isEqualTo(EncryptionConfigs.KEYSTORE_TYPE);
        assertThat(keyStoreType.required()).isFalse();
        assertThat(keyStoreType.multiple()).isFalse();
        assertThat(keyStoreType.mutable()).isTrue();
        assertThat(keyStoreType.defaultValue()).isEqualTo("JKS");
        assertThat(keyStoreType.type()).isEqualTo(ConfType.KEYSTORE_TYPE);

        ConfParameter keystorePath = configSpec.getParameter(EncryptionConfigs.KEYSTORE_PATH);
        assertThat(keystorePath.name()).isEqualTo(EncryptionConfigs.KEYSTORE_PATH);
        assertThat(keystorePath.required()).isTrue();
        assertThat(keystorePath.multiple()).isFalse();
        assertThat(keystorePath.mutable()).isTrue();
        assertThat(keystorePath.defaultValue()).isNull();
        assertThat(keystorePath.type()).isEqualTo(ConfType.FILE);

        ConfParameter keystorePassword =
                configSpec.getParameter(EncryptionConfigs.KEYSTORE_PASSWORD);
        assertThat(keystorePassword.name()).isEqualTo(EncryptionConfigs.KEYSTORE_PASSWORD);
        assertThat(keystorePassword.required()).isFalse();
        assertThat(keystorePassword.multiple()).isFalse();
        assertThat(keystorePassword.mutable()).isTrue();
        assertThat(keystorePassword.defaultValue()).isNull();
        assertThat(keystorePassword.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyPassword = configSpec.getParameter(EncryptionConfigs.KEY_PASSWORD);
        assertThat(keyPassword.name()).isEqualTo(EncryptionConfigs.KEY_PASSWORD);
        assertThat(keyPassword.required()).isFalse();
        assertThat(keyPassword.multiple()).isFalse();
        assertThat(keyPassword.mutable()).isTrue();
        assertThat(keyPassword.defaultValue()).isNull();
        assertThat(keyPassword.type()).isEqualTo(ConfType.TEXT);
    }
}
