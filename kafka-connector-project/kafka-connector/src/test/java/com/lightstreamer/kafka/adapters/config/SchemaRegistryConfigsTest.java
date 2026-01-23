
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.URL;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfParameter;
import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType;

import org.junit.jupiter.api.Test;

public class SchemaRegistryConfigsTest {

    @Test
    void shouldReturnConfigSpec() {
        ConfigsSpec configSpec = SchemaRegistryConfigs.spec();

        ConfParameter url = configSpec.getParameter(SchemaRegistryConfigs.URL);
        assertThat(url.name()).isEqualTo(SchemaRegistryConfigs.URL);
        assertThat(url.required()).isTrue();
        assertThat(url.multiple()).isFalse();
        assertThat(url.mutable()).isTrue();
        assertThat(url.defaultValue()).isNull();
        assertThat(url.type()).isEqualTo(URL);

        ConfParameter schemaRegistryProvider =
                configSpec.getParameter(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER);
        assertThat(schemaRegistryProvider.name())
                .isEqualTo(SchemaRegistryConfigs.SCHEMA_REGISTRY_PROVIDER);
        assertThat(schemaRegistryProvider.required()).isFalse();
        assertThat(schemaRegistryProvider.multiple()).isFalse();
        assertThat(schemaRegistryProvider.mutable()).isTrue();
        assertThat(schemaRegistryProvider.defaultValue()).isEqualTo("CONFLUENT");
        assertThat(schemaRegistryProvider.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslProtocol = configSpec.getParameter(SchemaRegistryConfigs.SSL_PROTOCOL);
        assertThat(sslProtocol.name()).isEqualTo(SchemaRegistryConfigs.SSL_PROTOCOL);
        assertThat(sslProtocol.required()).isFalse();
        assertThat(sslProtocol.multiple()).isFalse();
        assertThat(sslProtocol.mutable()).isTrue();
        assertThat(sslProtocol.defaultValue()).isEqualTo("TLSv1.3");
        assertThat(sslProtocol.type()).isEqualTo(ConfType.SSL_PROTOCOL);

        ConfParameter trustStoreType =
                configSpec.getParameter(SchemaRegistryConfigs.TRUSTSTORE_TYPE);
        assertThat(trustStoreType.name()).isEqualTo(SchemaRegistryConfigs.TRUSTSTORE_TYPE);
        assertThat(trustStoreType.required()).isFalse();
        assertThat(trustStoreType.multiple()).isFalse();
        assertThat(trustStoreType.mutable()).isTrue();
        assertThat(trustStoreType.defaultValue()).isEqualTo("JKS");
        assertThat(trustStoreType.type()).isEqualTo(ConfType.KEYSTORE_TYPE);

        ConfParameter trustStorePath =
                configSpec.getParameter(SchemaRegistryConfigs.TRUSTSTORE_PATH);
        assertThat(trustStorePath.name()).isEqualTo(SchemaRegistryConfigs.TRUSTSTORE_PATH);
        assertThat(trustStorePath.required()).isFalse();
        assertThat(trustStorePath.multiple()).isFalse();
        assertThat(trustStorePath.mutable()).isTrue();
        assertThat(trustStorePath.defaultValue()).isNull();
        assertThat(trustStorePath.type()).isEqualTo(ConfType.FILE);

        ConfParameter trustStorePassword =
                configSpec.getParameter(SchemaRegistryConfigs.TRUSTSTORE_PASSWORD);
        assertThat(trustStorePassword.name()).isEqualTo(SchemaRegistryConfigs.TRUSTSTORE_PASSWORD);
        assertThat(trustStorePassword.required()).isFalse();
        assertThat(trustStorePassword.multiple()).isFalse();
        assertThat(trustStorePassword.mutable()).isTrue();
        assertThat(trustStorePassword.defaultValue()).isNull();
        assertThat(trustStorePassword.type()).isEqualTo(ConfType.TEXT);

        ConfParameter enableHostNameVerification =
                configSpec.getParameter(SchemaRegistryConfigs.HOSTNAME_VERIFICATION_ENABLE);
        assertThat(enableHostNameVerification.name())
                .isEqualTo(SchemaRegistryConfigs.HOSTNAME_VERIFICATION_ENABLE);
        assertThat(enableHostNameVerification.required()).isFalse();
        assertThat(enableHostNameVerification.multiple()).isFalse();
        assertThat(enableHostNameVerification.mutable()).isTrue();
        assertThat(enableHostNameVerification.defaultValue()).isEqualTo("false");
        assertThat(enableHostNameVerification.type()).isEqualTo(ConfType.BOOL);

        ConfParameter sslCipherSuites =
                configSpec.getParameter(SchemaRegistryConfigs.SSL_CIPHER_SUITES);
        assertThat(sslCipherSuites.name()).isEqualTo(SchemaRegistryConfigs.SSL_CIPHER_SUITES);
        assertThat(sslCipherSuites.required()).isFalse();
        assertThat(sslCipherSuites.multiple()).isFalse();
        assertThat(sslCipherSuites.mutable()).isTrue();
        assertThat(sslCipherSuites.defaultValue()).isNull();
        assertThat(sslCipherSuites.type()).isEqualTo(ConfType.TEXT_LIST);

        ConfParameter sslProviders = configSpec.getParameter(SchemaRegistryConfigs.SSL_PROVIDER);
        assertThat(sslProviders.name()).isEqualTo(SchemaRegistryConfigs.SSL_PROVIDER);
        assertThat(sslProviders.required()).isFalse();
        assertThat(sslProviders.multiple()).isFalse();
        assertThat(sslProviders.mutable()).isTrue();
        assertThat(sslProviders.defaultValue()).isNull();
        assertThat(sslProviders.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslEngineFactoryClass =
                configSpec.getParameter(SchemaRegistryConfigs.SSL_ENGINE_FACTORY_CLASS);
        assertThat(sslEngineFactoryClass.name())
                .isEqualTo(SchemaRegistryConfigs.SSL_ENGINE_FACTORY_CLASS);
        assertThat(sslEngineFactoryClass.required()).isFalse();
        assertThat(sslEngineFactoryClass.multiple()).isFalse();
        assertThat(sslEngineFactoryClass.mutable()).isTrue();
        assertThat(sslEngineFactoryClass.defaultValue()).isNull();
        assertThat(sslEngineFactoryClass.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslKeyManagerAlgorithm =
                configSpec.getParameter(SchemaRegistryConfigs.SSL_KEYMANAGER_ALGORITHM);
        assertThat(sslKeyManagerAlgorithm.name())
                .isEqualTo(SchemaRegistryConfigs.SSL_KEYMANAGER_ALGORITHM);
        assertThat(sslKeyManagerAlgorithm.required()).isFalse();
        assertThat(sslKeyManagerAlgorithm.multiple()).isFalse();
        assertThat(sslKeyManagerAlgorithm.mutable()).isTrue();
        assertThat(sslKeyManagerAlgorithm.defaultValue()).isNull();
        assertThat(sslKeyManagerAlgorithm.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslSecureRandomImplementation =
                configSpec.getParameter(SchemaRegistryConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION);
        assertThat(sslSecureRandomImplementation.name())
                .isEqualTo(SchemaRegistryConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION);
        assertThat(sslSecureRandomImplementation.required()).isFalse();
        assertThat(sslSecureRandomImplementation.multiple()).isFalse();
        assertThat(sslSecureRandomImplementation.mutable()).isTrue();
        assertThat(sslSecureRandomImplementation.defaultValue()).isNull();
        assertThat(sslSecureRandomImplementation.type()).isEqualTo(ConfType.TEXT);

        ConfParameter sslTrustManagerAlgorithm =
                configSpec.getParameter(SchemaRegistryConfigs.SSL_TRUSTMANAGER_ALGORITHM);
        assertThat(sslTrustManagerAlgorithm.name())
                .isEqualTo(SchemaRegistryConfigs.SSL_TRUSTMANAGER_ALGORITHM);
        assertThat(sslTrustManagerAlgorithm.required()).isFalse();
        assertThat(sslTrustManagerAlgorithm.multiple()).isFalse();
        assertThat(sslTrustManagerAlgorithm.mutable()).isTrue();
        assertThat(sslTrustManagerAlgorithm.defaultValue()).isNull();
        assertThat(sslTrustManagerAlgorithm.type()).isEqualTo(ConfType.TEXT);

        ConfParameter securityProviders =
                configSpec.getParameter(SchemaRegistryConfigs.SECURITY_PROVIDERS);
        assertThat(securityProviders.name()).isEqualTo(SchemaRegistryConfigs.SECURITY_PROVIDERS);
        assertThat(securityProviders.required()).isFalse();
        assertThat(securityProviders.multiple()).isFalse();
        assertThat(securityProviders.mutable()).isTrue();
        assertThat(securityProviders.defaultValue()).isNull();
        assertThat(securityProviders.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyStoreType = configSpec.getParameter(SchemaRegistryConfigs.KEYSTORE_TYPE);
        assertThat(keyStoreType.name()).isEqualTo(SchemaRegistryConfigs.KEYSTORE_TYPE);
        assertThat(keyStoreType.required()).isFalse();
        assertThat(keyStoreType.multiple()).isFalse();
        assertThat(keyStoreType.mutable()).isTrue();
        assertThat(keyStoreType.defaultValue()).isEqualTo("JKS");
        assertThat(keyStoreType.type()).isEqualTo(ConfType.KEYSTORE_TYPE);

        ConfParameter keystorePath = configSpec.getParameter(SchemaRegistryConfigs.KEYSTORE_PATH);
        assertThat(keystorePath.name()).isEqualTo(SchemaRegistryConfigs.KEYSTORE_PATH);
        assertThat(keystorePath.required()).isTrue();
        assertThat(keystorePath.multiple()).isFalse();
        assertThat(keystorePath.mutable()).isTrue();
        assertThat(keystorePath.defaultValue()).isNull();
        assertThat(keystorePath.type()).isEqualTo(ConfType.FILE);

        ConfParameter keystorePassword =
                configSpec.getParameter(SchemaRegistryConfigs.KEYSTORE_PASSWORD);
        assertThat(keystorePassword.name()).isEqualTo(SchemaRegistryConfigs.KEYSTORE_PASSWORD);
        assertThat(keystorePassword.required()).isFalse();
        assertThat(keystorePassword.multiple()).isFalse();
        assertThat(keystorePassword.mutable()).isTrue();
        assertThat(keystorePassword.defaultValue()).isNull();
        assertThat(keystorePassword.type()).isEqualTo(ConfType.TEXT);

        ConfParameter keyPassword = configSpec.getParameter(SchemaRegistryConfigs.KEY_PASSWORD);
        assertThat(keyPassword.name()).isEqualTo(SchemaRegistryConfigs.KEY_PASSWORD);
        assertThat(keyPassword.required()).isFalse();
        assertThat(keyPassword.multiple()).isFalse();
        assertThat(keyPassword.mutable()).isTrue();
        assertThat(keyPassword.defaultValue()).isNull();
        assertThat(keyPassword.type()).isEqualTo(ConfType.TEXT);

        ConfParameter azureSchemaIdHeader =
                configSpec.getParameter(SchemaRegistryConfigs.AZURE_SCHEMA_ID_HEADER);
        assertThat(azureSchemaIdHeader.name())
                .isEqualTo(SchemaRegistryConfigs.AZURE_SCHEMA_ID_HEADER);
        assertThat(azureSchemaIdHeader.required()).isFalse();
        assertThat(azureSchemaIdHeader.multiple()).isFalse();
        assertThat(azureSchemaIdHeader.mutable()).isTrue();
        assertThat(azureSchemaIdHeader.defaultValue()).isEqualTo("");
        assertThat(azureSchemaIdHeader.type()).isEqualTo(ConfType.TEXT);

        ConfParameter azureTenantId =
                configSpec.getParameter(SchemaRegistryConfigs.AZURE_TENANT_ID);
        assertThat(azureTenantId.name()).isEqualTo(SchemaRegistryConfigs.AZURE_TENANT_ID);
        assertThat(azureTenantId.required()).isFalse();
        assertThat(azureTenantId.multiple()).isFalse();
        assertThat(azureTenantId.mutable()).isTrue();
        assertThat(azureTenantId.defaultValue()).isNull();
        assertThat(azureTenantId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter azureClientId =
                configSpec.getParameter(SchemaRegistryConfigs.AZURE_CLIENT_ID);
        assertThat(azureClientId.name()).isEqualTo(SchemaRegistryConfigs.AZURE_CLIENT_ID);
        assertThat(azureClientId.required()).isFalse();
        assertThat(azureClientId.multiple()).isFalse();
        assertThat(azureClientId.mutable()).isTrue();
        assertThat(azureClientId.defaultValue()).isNull();
        assertThat(azureClientId.type()).isEqualTo(ConfType.TEXT);

        ConfParameter azureClientSecret =
                configSpec.getParameter(SchemaRegistryConfigs.AZURE_CLIENT_SECRET);
        assertThat(azureClientSecret.name()).isEqualTo(SchemaRegistryConfigs.AZURE_CLIENT_SECRET);
        assertThat(azureClientSecret.required()).isFalse();
        assertThat(azureClientSecret.multiple()).isFalse();
        assertThat(azureClientSecret.mutable()).isTrue();
        assertThat(azureClientSecret.defaultValue()).isNull();
        assertThat(azureClientSecret.type()).isEqualTo(ConfType.TEXT);
    }
}
