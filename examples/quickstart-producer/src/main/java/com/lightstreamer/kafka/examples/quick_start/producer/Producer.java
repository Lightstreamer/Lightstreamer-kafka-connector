
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

package com.lightstreamer.kafka.examples.quick_start.producer;

import com.lightstreamer.kafka.examples.quick_start.producer.json.JsonStock;
import com.lightstreamer.kafka.examples.quick_start.producer.protobuf.ProtobufStock;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

public class Producer implements Runnable, FeedSimulator.ExternalFeedListener {

    enum SerializerType {
        PROTOBUF,
        JSON,
        JSON_SCHEMA;

        public static SerializerType fromString(String className) {
            if (KafkaProtobufSerializer.class.getName().equals(className)) {
                return PROTOBUF;
            } else if (KafkaJsonSerializer.class.getName().equals(className)) {
                return JSON;
            } else if (KafkaJsonSchemaSerializer.class.getName().equals(className)) {
                return JSON_SCHEMA;
            }
            throw new IllegalArgumentException("Unknown serializer class: " + className);
        }
    }

    @Option(
            names = "--bootstrap-servers",
            description = "The Kafka connection string",
            required = true)
    private String bootstrapServers;

    @Option(names = "--topic", description = "The target topic", required = true)
    private String topic;

    @Option(
            names = "--config-file",
            description = "Optional producer config file",
            required = false)
    private String configFile;

    private KafkaProducer<Integer, Object> producer;

    private SerializerType serializerType;

    public Producer() {}

    public Producer(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    SerializerType getSerializerType() {
        return serializerType;
    }

    @Override
    public void run() {
        // Create producer configs
        Properties properties = getProperties();

        configure(properties);

        // Create the producer
        producer = new KafkaProducer<>(properties);

        // Create and start the feed simulator.
        FeedSimulator simulator = new FeedSimulator(this);
        simulator.start();
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        if (configFile != null) {
            if (!Files.exists(Paths.get(configFile))) {
                System.err.println("Unable to find the specified configuration file " + configFile);
                System.exit(-1);
            }
            try (InputStream is = Files.newInputStream(Paths.get(configFile))) {
                properties.load(is);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        return properties;
    }

    void configure(Properties properties) {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        if (!properties.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            throw new RuntimeException(
                    "Please specify the value serializer class in the configuration file");
        }
        serializerType =
                SerializerType.fromString(
                        properties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Override
    public void onEvent(int stockIndex, Map<String, String> stockEvent) {
        ProducerRecord<Integer, Object> record =
                new ProducerRecord<>(this.topic, stockIndex, toRecord(stockEvent));
        producer.send(
                record,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                            System.err.println("Send failed");
                            return;
                        }
                        String reducedValueStr = record.value().toString().substring(0, 35) + "...";
                        System.out.printf(
                                "Sent record [key = %2d, offset = %6d, value = %s]%n",
                                record.key(), metadata.offset(), reducedValueStr);
                    }
                });
    }

    Object toRecord(Map<String, String> stockEvent) {
        return switch (serializerType) {
            case JSON, JSON_SCHEMA -> JsonStock.fromEvent(stockEvent);
            case PROTOBUF -> ProtobufStock.fromEvent(stockEvent);
        };
    }

    public static void main(String[] args) {
        new CommandLine(new Producer()).execute(args);
    }
}
