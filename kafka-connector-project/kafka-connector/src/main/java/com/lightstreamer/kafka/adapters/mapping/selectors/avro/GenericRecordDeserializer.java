
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
import com.lightstreamer.kafka.common.config.ConfigException;

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

public class GenericRecordDeserializer {

    // private Deserializer<?> deserializer;
    // private final boolean isKey;

    // GenericRecordDeserializer(ConnectorConfig config, boolean isKey) {
    //     this.isKey = isKey;
    //     if ((isKey && config.hasKeySchemaFile()) || (!isKey && config.hasValueSchemaFile())) {
    //         deserializer = new GenericRecordLocalSchemaDeserializer(config, isKey);
    //     } else {
    //         if (isKey) {
    //             if (config.isSchemaRegistryEnabledForKey()) {
    //                 deserializer = new KafkaAvroDeserializer();
    //             }
    //         } else {
    //             if (config.isSchemaRegistryEnabledForValue()) {
    //                 deserializer = new KafkaAvroDeserializer();
    //             }
    //         }
    //     }
    //     if (deserializer == null) {
    //         throw new ConfigException("No AVRO deserializer could be instantiated");
    //     }
    //     deserializer.configure(Utils.propsToMap(config.baseConsumerProps()), isKey);
    // }

    // public boolean isKey() {
    //     return isKey;
    // }

    // public String deserializerClassName() {
    //     return deserializer.getClass().getName();
    // }

    // @Override
    // public GenericRecord deserialize(String topic, byte[] data) {
    //     GenericRecord deserialize = (GenericRecord) deserializer.deserialize(topic, data);
    //     return deserialize;
    // }

    // @Override
    // public void close() {
    //     deserializer.close();
    // }

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
                throw new SerializationException(e.getMessage());
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
                throw new RuntimeException(e.getMessage());
            }
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
            GenericRecordLocalSchemaDeserializer genericRecordLocalDeserializer =
                    new GenericRecordLocalSchemaDeserializer();
            genericRecordLocalDeserializer.preConfigure(config);
            return genericRecordLocalDeserializer;
        }
        if ((isKey && config.isSchemaRegistryEnabledForKey())
                || (!isKey && config.isSchemaRegistryEnabledForValue())) {
            return new WrapperKafkaAvroDeserializer();
        }

        throw new ConfigException("No AVRO deserializer could be instantiated");
    }

    private static void checkEvaluator(ConnectorConfig config, boolean isKey) {
        if (isJsonKeyEvaluator(config, isKey)) {
            return;
        }
        if (isJsonValueEvaluator(config, isKey)) {
            return;
        }
        throw new IllegalArgumentException("Evaluator type is not AVRO");
    }

    private static boolean isJsonValueEvaluator(ConnectorConfig config, boolean isKey) {
        if (isKey) {
            return false;
        }
        return config.getValueEvaluator().is(AVRO);
    }

    private static boolean isJsonKeyEvaluator(ConnectorConfig config, boolean isKey) {
        if (!isKey) {
            return false;
        }
        return config.getKeyEvaluator().is(AVRO);
    }
}
