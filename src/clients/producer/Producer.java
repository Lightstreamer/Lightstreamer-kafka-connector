
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

package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import picocli.CommandLine.Option;

import java.security.SecureRandom;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Producer implements Runnable {

    @Option(
            names = "--bootstrap-servers",
            description = "The Kafka connection string",
            required = true)
    private String bootstrapServers;

    @Option(names = "--topic", description = "The target topic", required = true)
    private String topic;

    @Option(
            names = "--period",
            description = "The interval in ms between two successive executions",
            required = false,
            defaultValue = "250")
    private int periodMs;

    public void run() {
        // BasicConfigurator.configure();
        // Create producer configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        // properties.setProperty(
        //         SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
        // "secrets/kafka.client.truststore.jks");
        // properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password");
        // properties.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        // ssl.truststore.password=test1234
        // Create and start the producer.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties); ) {
            int key = 0;
            while (true) {
                String message =
                        new SecureRandom()
                                .ints(20, 48, 122)
                                .mapToObj(Character::toString)
                                .collect(Collectors.joining());

                String keyString = null; // String.valueOf(key++);
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(this.topic, keyString, message);
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
                                        "Sent record [%s]%n to topic [%s] and partition [%d]%n",
                                        record.value(), record.topic(), record.partition());
                            }
                        });
                TimeUnit.MILLISECONDS.sleep(this.periodMs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
