
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.mapping.selectors;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;

import org.apache.kafka.common.serialization.Deserializer;

public abstract class AbstractAzureSchemaRegistryDeserializer<T> implements Deserializer<T> {

    private final ConnectorConfig config;
    private final TokenCredential tokenCredential;

    public AbstractAzureSchemaRegistryDeserializer(ConnectorConfig config) {
        this.config = config;
        this.tokenCredential =
                new ClientSecretCredentialBuilder()
                        .tenantId(config.azureTenantId())
                        .clientId(config.azureClientId())
                        .clientSecret(config.azureClientSecret())
                        .build();
    }

    protected ConnectorConfig getConfig() {
        return config;
    }

    protected TokenCredential getTokenCredential() {
        return tokenCredential;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        throw new UnsupportedOperationException("Unimplemented method 'deserialize'");
    }
}
