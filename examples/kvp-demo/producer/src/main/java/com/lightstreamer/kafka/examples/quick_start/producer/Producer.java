
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Producer implements Runnable, FeedSimulator.ExternalFeedListener {

    @Option(names = "--bootstrap-servers", description = "The Kafka connection string", required = true)
    private String bootstrapServers;

    @Option(names = "--config-file", description = "Optional producer config file", required = false)
    private String configFile;

    private KafkaProducer<String, String> producer;

    public void run() {
        // Create producer configs
        Properties properties = new Properties();
        if (configFile != null) {
            if (!Files.exists(Paths.get(configFile))) {
                System.err.println("Unable to find the specifed configuration file " + configFile);
                System.exit(-1);
            }
            try (InputStream is = Files.newInputStream(Paths.get(configFile))) {
                properties.load(is);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        String serializer = Serdes.String().serializer().getClass().getName();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);

        // Create the producer
        producer = new KafkaProducer<>(properties);

        // Create and start the feed simulator.
        FeedSimulator simulator = new FeedSimulator(this);
        simulator.start();
    }

    @Override
    public void onEvent(String topic, String symbol, String quote) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "", quote);
            producer.send(
                    record,
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                                System.err.println("Send failed");
                                return;
                            }
                            String reducedValueStr = record.value().substring(0, 35) + "...";
                            System.out.printf(
                                    "Sent record [topic = %2s, offset = %6d, value = %s]%n",
                                    record.topic(), metadata.offset(), reducedValueStr);
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new CommandLine(new Producer()).execute(args);
    }
}
