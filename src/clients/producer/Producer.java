
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

import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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
            names = "--config-path",
            description = "The configuration file path",
            required = false,
            defaultValue = "src/clients/producer/simple-config.properties")
    private String configPath;

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
        System.out.println(Paths.get(".").toAbsolutePath());
        try (InputStream is = Files.newInputStream(Paths.get(this.configPath))) {
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Create and start the producer.

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties); ) {
            int key = 0;
            while (true) {
                String message =
                        new SecureRandom()
                                .ints(20, 48, 122)
                                .mapToObj(Character::toString)
                                .collect(Collectors.joining());

                String keyString = String.valueOf(key++);
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
                                        "Sent record [key=%s,value=%s]%n to topic [%s]]%n",
                                        record.key(),
                                        record.value(),
                                        record.topic(),
                                        record.partition());
                            }
                        });
                TimeUnit.MILLISECONDS.sleep(this.periodMs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
