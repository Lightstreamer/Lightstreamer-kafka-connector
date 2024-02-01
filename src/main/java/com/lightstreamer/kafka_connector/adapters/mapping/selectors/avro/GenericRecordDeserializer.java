
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

package com.lightstreamer.kafka_connector.adapters.mapping.selectors.avro;

import com.lightstreamer.kafka_connector.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.AbstractLocalSchemaDeserializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class GenericRecordDeserializer implements Deserializer<GenericRecord> {

    private Deserializer<?> deserializer;
    private final boolean isKey;

    GenericRecordDeserializer(ConnectorConfig config, boolean isKey) {
        Map<String, String> props = new HashMap<>();
        this.isKey = isKey;
        if ((isKey && config.hasKeySchemaFile()) || (!isKey && config.hasValueSchemaFile())) {
            deserializer = new GenericRecordLocalSchemaDeserializer(config, isKey);
        } else {
            String schemaRegistryKey =
                    isKey
                            ? ConnectorConfig.KEY_EVALUATOR_SCHEMA_REGISTRY_URL
                            : ConnectorConfig.VALUE_EVALUATOR_SCHEMA_REGISTRY_URL;
            props.put(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    config.getUrl(schemaRegistryKey, true));
            deserializer = new KafkaAvroDeserializer();
        }
        deserializer.configure(config.extendsConsumerProps(props), isKey);
    }

    public boolean isKey() {
        return isKey;
    }

    public String deserializerClassName() {
        return deserializer.getClass().getName();
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        GenericRecord deserialize = (GenericRecord) deserializer.deserialize(topic, data);
        return deserialize;
    }
}

class GenericRecordLocalSchemaDeserializer extends AbstractLocalSchemaDeserializer<GenericRecord> {

    private Schema schema;

    GenericRecordLocalSchemaDeserializer(ConnectorConfig config, boolean isKey) {
        super(config, isKey);
        try {
            schema = new Schema.Parser().parse(schemaFile);
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
