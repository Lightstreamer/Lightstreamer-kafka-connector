
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

package com.lightstreamer.kafka.adapters.mapping.selectors.avro;

import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.AVRO;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.SchemaRegistryConfigs;
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractLocalSchemaDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GenericRecordDeserializers {

    private static final String SCHEMA_REGISTRY_PROVIDER_AZURE = "AZURE";

    static class WrapperKafkaAvroDeserializer implements Deserializer<GenericRecord> {

        private final KafkaAvroDeserializer innerDeserializer;

        WrapperKafkaAvroDeserializer() {
            this.innerDeserializer = new KafkaAvroDeserializer();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            innerDeserializer.configure(configs, isKey);
        }

        @Override
        public GenericRecord deserialize(String topic, byte[] data) {
            return (GenericRecord) innerDeserializer.deserialize(topic, data);
        }
    }

    static class GenericRecordLocalSchemaDeserializer
            extends AbstractLocalSchemaDeserializer<GenericRecord> {

        private Schema schema;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            try {
                schema = new Schema.Parser().parse(getSchemaFile(isKey));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public GenericRecord deserialize(String topic, byte[] data) {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            // ByteBuffer b = ByteBuffer.wrap(data);
            // b.position(5);
            // byte[] newBytes = new byte[data.length - 5];
            // b.get(newBytes);
            BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(data, null);
            try {
                return datumReader.read(null, binaryDecoder);
            } catch (IOException e) {
                throw new SerializationException(e.getMessage());
            }
        }
    }

    static class AzureSchemaRegistryDeserializer implements Deserializer<GenericRecord> {

        private com.azure.data.schemaregistry.SchemaRegistryClient schemaRegistryClient;
        private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
        private String schemaIdHeader;
        private String schemaRegistryNamespace;
        private String tenantId;
        private String clientId;
        private String clientSecret;

        public void preConfigure(ConnectorConfig config, boolean isKey) {
            // Retrieve the header name from configuration
            schemaIdHeader = config.azureSchemaIdHeader();
            if (schemaIdHeader == null || schemaIdHeader.isBlank()) {
                schemaIdHeader = ""; // fallback to default
            }

            // Create the SchemaRegistryClient using configurations
            schemaRegistryNamespace = config.schemaRegistryUrl();
            if (schemaRegistryNamespace != null && schemaRegistryNamespace.startsWith("https://")) {
                schemaRegistryNamespace = schemaRegistryNamespace.substring(8);
            }
            if (schemaRegistryNamespace != null && schemaRegistryNamespace.endsWith("/")) {
                schemaRegistryNamespace = schemaRegistryNamespace.substring(0, schemaRegistryNamespace.length() - 1);
            }
            
            // If specified, use Service Principal credentials
            tenantId = config.azureTenantId();
            clientId = config.azureClientId();
            clientSecret = config.azureClientSecret();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // Initialize the Azure Schema Registry client
            com.azure.identity.DefaultAzureCredentialBuilder credentialBuilder = 
                new com.azure.identity.DefaultAzureCredentialBuilder();
            
            com.azure.core.credential.TokenCredential credential;
            if (clientId != null && clientSecret != null && tenantId != null) {
                credential = new com.azure.identity.ClientSecretCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .build();
            } else {
                credential = credentialBuilder.build();
            }
            
            this.schemaRegistryClient = new com.azure.data.schemaregistry.SchemaRegistryClientBuilder()
                .fullyQualifiedNamespace(schemaRegistryNamespace)
                .credential(credential)
                .buildClient();
        }

        @Override
        public GenericRecord deserialize(String topic, byte[] data) {
            return deserialize(topic, null, data);
        }

        @Override
        public GenericRecord deserialize(String topic, Headers headers, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            
            // Extract the schema ID from the header
            String schemaId = resolveSchemaId(headers);
            if (schemaId == null || schemaId.isBlank()) {
                throw new SerializationException("Schema ID header not found");
            }
            
            try {
                Schema schema = schemaCache.computeIfAbsent(schemaId, this::resolveSchema);
                
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                return reader.read(null, decoder);
            } catch (IOException ex) {
                throw new SerializationException("Failed to decode Avro payload", ex);
            }
        }
        
        private String resolveSchemaId(Headers headers) {
            if (headers == null) {
                return null;
            }
            org.apache.kafka.common.header.Header header = headers.lastHeader(schemaIdHeader);
            if (header == null || header.value() == null) {
                return null;
            }
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        
        private Schema resolveSchema(String schemaId) {
            com.azure.data.schemaregistry.models.SchemaRegistrySchema schema = 
                schemaRegistryClient.getSchema(schemaId);
            return new Schema.Parser().parse(schema.getDefinition());
        }

        @Override
        public void close() {
            // The client does not have an explicit close method
        }
    }

    static Deserializer<GenericRecord> ValueDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, false);
    }

    static Deserializer<GenericRecord> KeyDeserializer(ConnectorConfig config) {
        return configuredDeserializer(config, true);
    }

    private static Deserializer<GenericRecord> configuredDeserializer(
            ConnectorConfig config, boolean isKey) {
        checkEvaluator(config, isKey);
        Deserializer<GenericRecord> deserializer = newDeserializer(config, isKey);
        
        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            if (SCHEMA_REGISTRY_PROVIDER_AZURE.equalsIgnoreCase(config.schemaRegistryProvider())) {
                deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
            } else {
                deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
            }
        } else {
            deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
        }
        
        return deserializer;
    }

    private static Deserializer<GenericRecord> newDeserializer(
            ConnectorConfig config, boolean isKey) {
        if ((isKey && config.hasKeySchemaFile()) || (!isKey && config.hasValueSchemaFile())) {
            GenericRecordLocalSchemaDeserializer localSchemaDeser =
                    new GenericRecordLocalSchemaDeserializer();
            localSchemaDeser.preConfigure(config, isKey);
            return localSchemaDeser;
        }
        
        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            if (SCHEMA_REGISTRY_PROVIDER_AZURE.equalsIgnoreCase(config.schemaRegistryProvider())) {
                AzureSchemaRegistryDeserializer azureDeserializer = new AzureSchemaRegistryDeserializer();
                azureDeserializer.preConfigure(config, isKey);
                return azureDeserializer;
            } else {
                return new WrapperKafkaAvroDeserializer();
            }
        }

        return new WrapperKafkaAvroDeserializer();
    }

    private static void checkEvaluator(ConnectorConfig config, boolean isKey) {
        if (isAvroKeyEvaluator(config, isKey)) {
            return;
        }
        if (isAvroValueEvaluator(config, isKey)) {
            return;
        }
        throw new IllegalArgumentException("Evaluator type is not AVRO");
    }

    private static boolean isAvroKeyEvaluator(ConnectorConfig config, boolean isKey) {
        if (!isKey) {
            return false;
        }
        return config.getKeyEvaluator().is(AVRO);
    }

    private static boolean isAvroValueEvaluator(ConnectorConfig config, boolean isKey) {
        if (isKey) {
            return false;
        }
        return config.getValueEvaluator().is(AVRO);
    }
}
