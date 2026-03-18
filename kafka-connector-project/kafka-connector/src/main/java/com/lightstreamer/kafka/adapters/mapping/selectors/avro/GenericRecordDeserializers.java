
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
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SchemaRegistryProvider.AZURE;

import com.azure.core.models.MessageContent;
import com.azure.core.util.BinaryData;
import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializerBuilder;
import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractAzureSchemaRegistryDeserializer;
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractLocalSchemaDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class GenericRecordDeserializers {

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

        @Override
        public void close() {
            innerDeserializer.close();
        }
    }

    static class GenericRecordLocalSchemaDeserializer
            extends AbstractLocalSchemaDeserializer<GenericRecord> {

        private Schema schema;

        GenericRecordLocalSchemaDeserializer(ConnectorConfig config) {
            super(config);
        }

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
            if (data == null || data.length == 0) {
                return null;
            }
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

    static class GenericRecordAzureSchemaRegistryDeserializer
            extends AbstractAzureSchemaRegistryDeserializer<GenericRecord> {

        private SchemaRegistryApacheAvroSerializer serializer;

        GenericRecordAzureSchemaRegistryDeserializer(ConnectorConfig config) {
            super(config);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.serializer =
                    new SchemaRegistryApacheAvroSerializerBuilder()
                            .schemaRegistryClient(
                                    new SchemaRegistryClientBuilder()
                                            .fullyQualifiedNamespace(
                                                    getConfig().schemaRegistryUrl())
                                            .credential(getTokenCredential())
                                            .buildAsyncClient())
                            .buildSerializer();
        }

        @Override
        public GenericRecord deserialize(String topic, Headers headers, byte[] data) {
            if (data == null || data.length == 0) {
                return null;
            }
            Header contentTypeHeader = headers.lastHeader("content-type");
            MessageContent message =
                    new MessageContent()
                            .setBodyAsBinaryData(BinaryData.fromBytes(data))
                            .setContentType(
                                    contentTypeHeader != null
                                            ? new String(contentTypeHeader.value())
                                            : "");
            // SchemaRegistryApacheAvroSerializer.deserialize() calls block() with no timeout,
            // which can hang indefinitely when the schema registry URL is wrong or unreachable.
            // Using deserializeAsync() with a Reactor timeout() before block() guarantees the
            // call is always bounded, regardless of transport state.
            return serializer
                    .deserializeAsync(message, TypeReference.createInstance(GenericRecord.class))
                    .timeout(Duration.ofSeconds(2))
                    .block();
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
        deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
        return deserializer;
    }

    private static Deserializer<GenericRecord> newDeserializer(
            ConnectorConfig config, boolean isKey) {
        if ((isKey && config.hasKeySchemaFile()) || (!isKey && config.hasValueSchemaFile())) {
            return new GenericRecordLocalSchemaDeserializer(config);
        }

        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            if (AZURE.equals(config.schemaRegistryProvider())) {
                return new GenericRecordAzureSchemaRegistryDeserializer(config);
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
