
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
import com.lightstreamer.kafka.adapters.mapping.selectors.AbstractLocalSchemaDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.Map;

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

        private final com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializer
                delegate = new com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializer();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            delegate.configure(configs, isKey);
        }

        @Override
        public GenericRecord deserialize(String topic, byte[] data) {
            System.out.println("AzureSchemaRegistryDeserializer: Deserializing data for topic " + topic);
            return (GenericRecord) delegate.deserialize(topic, data);
        }

        @Override
        public GenericRecord deserialize(String topic, org.apache.kafka.common.header.Headers headers, byte[] data) {
            System.out.println("AzureSchemaRegistryDeserializer: Deserializing data for topic " + topic + " with headers " + headers);
            return (GenericRecord) delegate.deserialize(topic, headers, data);
        }

        @Override
        public void close() {
            delegate.close();
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
        
        Map<String, Object> props = Utils.propsToMap(config.baseConsumerProps());
        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            if (SCHEMA_REGISTRY_PROVIDER_AZURE.equalsIgnoreCase(config.schemaRegistryProvider())) {
                props.put("use.azure.credential", true);
            }
        }
        deserializer.configure(props, isKey);
        
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
                return new AzureSchemaRegistryDeserializer();
            }
            return new WrapperKafkaAvroDeserializer();
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
