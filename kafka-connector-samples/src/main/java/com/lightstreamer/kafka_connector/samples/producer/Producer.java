
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

package com.lightstreamer.kafka_connector.samples.producer;

import com.lightstreamer.kafka_connector.samples.producer.FeedSimluator.ExternalFeedListener;
import com.lightstreamer.kafka_connector.samples.producer.FeedSimluator.Stock;

import io.confluent.kafka.serializers.KafkaJsonSerializer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.Properties;

public class Producer implements Runnable, ExternalFeedListener {

    @Option(
            names = "--bootstrap-servers",
            description = "The Kafka connection string",
            required = true)
    private String bootstrapServers;

    @Option(names = "--topic", description = "The target topic", required = true)
    private String topic;

    private KafkaProducer<String, Stock> producer;

    public void run() {
        // Create producer configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        // Create the producer
        producer = new KafkaProducer<String, Stock>(properties);

        // Create and start the feed simulator.
        FeedSimluator simulator = new FeedSimluator();
        simulator.setFeedListener(this);
        simulator.start();
    }

    @Override
    public void onEvent(Stock stock, boolean b) {
        ProducerRecord<String, Stock> record =
                new ProducerRecord<>(this.topic, stock.name.replace(' ', '-'), stock);
        producer.send(
                record,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                            System.err.println("Send failed");
                            return;
                        }
                        System.out.printf(
                                "Sent record [key=%s,value=%s]%n to topic [%s]]%n",
                                record.key(), record.value(), record.topic(), record.partition());
                    }
                });
    }

    public static void main(String[] args) {
        new CommandLine(new Producer()).execute(args);
    }
}
