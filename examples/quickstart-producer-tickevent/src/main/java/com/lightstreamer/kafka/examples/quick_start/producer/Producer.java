
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

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.example.tutorial.protos.TickEvent;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * A Kafka producer implementation that simulates and publishes crypto market events to a specified
 * topic. This class handles the initialization of a Kafka producer, configuration loading, and
 * event publishing. It can be run as a standalone application using command-line parameters or
 * instantiated programmatically.
 *
 * <p>Configuration can be provided either through command-line arguments or a configuration file.
 * Required command-line arguments:
 *
 * <ul>
 *   <li>--bootstrap-servers: The Kafka connection string
 *   <li>--topic: The target topic to publish messages to
 * </ul>
 *
 * Optional command-line argument:
 *
 * <ul>
 *   <li>--config-file: Path to a producer configuration file
 * </ul>
 *
 * <p>This producer works with a {@code FeedSimulator} to generate crypto events and publish them to
 * the specified Kafka topic.
 *
 * @see FeedSimulator.ExternalFeedListener
 * @see KafkaProducer
 */
public class Producer implements Runnable, FeedSimulator.ExternalFeedListener {

    @Option(
            names = "--bootstrap-servers",
            description = "The Kafka connection string",
            required = false)
    private String bootstrapServers = "localhost:9092";

    @Option(names = "--topic", description = "The target topic", required = true)
    private String topic;

    @Option(
            names = "--config-file",
            description = "Optional producer config file",
            required = false)
    private String configFile;

    private KafkaProducer<String, TickEvent> producer;

    public Producer() {}

    public Producer(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    /**
     * Entrypoint for the producer application invoked by the {@code picocli} framework.
     *
     * <p>This method initializes the Kafka producer with the specified configuration, loads
     * properties from the configuration file (if provided), and starts the feed simulator to
     * generate and send stock events.
     */
    @Override
    public void run() {
        // Create producer configs
        Properties configs = loadProperties();
        configure(configs);

        // Create the producer
        producer = new KafkaProducer<>(configs);

        // Create and start the feed simulator
        FeedSimulator simulator = new FeedSimulator(this);
        simulator.start();
    }

    /**
     * Loads properties from the specified configuration file.
     *
     * <p>This method creates a new Properties object and attempts to populate it from the file path
     * stored in the {@code configFile} field if it's not null. If the file doesn't exist or cannot
     * be read, the program will terminate with an error message.
     *
     * @return a Properties object containing configuration properties loaded from the file, or an
     *     empty Properties object if no configuration file was specified
     */
    private Properties loadProperties() {
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

    /**
     * Configures the Kafka Producer with necessary properties.
     *
     * <p>This method sets up the essential configuration properties for a Kafka producer:
     *
     * <ul>
     *   <li>Bootstrap servers - the Kafka cluster address
     *   <li>Key serializer - set to {@link StringSerializer}
     *   <li>Value serializer - set to {@link CustomProtobufSerializer}
     * </ul>
     *
     * @param config the Properties object to be configured with Kafka producer settings
     */
    void configure(Properties config) {
        config.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, CustomProtobufSerializer.class.getName());
    }

    @Override
    public void onEvent(TickEvent ticketEvent) {
        String symbol = ticketEvent.getTick().getSymbol();
        System.out.printf(
                "Receiving event for symbol %s on topic '%s': %s%n",
                symbol, this.topic, ticketEvent.toString());
        ProducerRecord<String, TickEvent> record =
                new ProducerRecord<>(this.topic, symbol, ticketEvent);
        try {
            producer.send(
                    record,
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if (e != null) {
                                System.err.printf("Send failed:%n%s", e.getMessage());
                                return;
                            }
                            System.out.printf(
                                    """
                                Sent record:
                                [
                                    KEY   => %s,
                                    VALUE => {%s}
                                ]%n
                                """,
                                    record.key(), record.value().toString());
                        }
                    });
        } catch (Exception e) {
            System.err.printf("Error sending records: %s%n", e.getMessage());
        }
    }

    public static void main(String[] args) {
        new CommandLine(new Producer()).execute(args);
    }
}
