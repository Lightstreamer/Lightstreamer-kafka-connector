
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

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

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
import java.util.Set;

/**
 * A Kafka producer implementation that simulates and publishes stock market events to a specified
 * topic. This class handles the initialization of a Kafka producer, configuration loading, and
 * event publishing. It can be run as a standalone application using command-line parameters or
 * instantiated programmatically.
 *
 * <p>The producer supports different serialization formats for the messages:
 *
 * <ul>
 *   <li>Protocol Buffers
 *   <li>JSON
 *   <li>JSON Schema
 * </ul>
 *
 * <p>Configuration can be provided either through command-line arguments or a configuration file.
 * Required command-line argument:
 *
 * <ul>
 *   <li>{@code --topic}: The target topic to publish messages to
 * </ul>
 *
 * Optional command-line arguments:
 *
 * <ul>
 *   <li>{@code --bootstrap-servers}: The Kafka connection string (can also be supplied via config
 *       file)
 *   <li>{@code --config-file}: Path to a producer configuration file
 * </ul>
 *
 * <p>This producer works with a {@code FeedSimulator} to generate stock events and publish them to
 * the specified Kafka topic.
 *
 * @see FeedSimulator.ExternalFeedListener
 * @see KafkaProducer
 * @see SerializationFormat
 */
public class Producer implements Runnable, FeedSimulator.ExternalFeedListener {

    /**
     * The property key used by the Azure Schema Registry Kafka serializer to obtain credentials.
     */
    static final String AZURE_SCHEMA_REGISTRY_CREDENTIAL = "schema.registry.credential";

    /**
     * Enum representing the serialization format used for message values in the Kafka producer. The
     * format is determined based on the class name of the value serializer specified in the
     * configuration.
     */
    enum SerializationFormat {
        PROTOBUF,
        AVRO,
        JSON,
        JSON_SCHEMA;

        /**
         * Converts a raw stock event into the appropriately-typed message value for this
         * serialization format.
         *
         * @param stockEvent a map of stock field names to their string values
         * @return the message value object ready to be placed in a {@link
         *     org.apache.kafka.clients.producer.ProducerRecord}
         */
        Object toValue(Map<String, String> stockEvent) {
            return switch (this) {
                case PROTOBUF -> StockValueFactory.protobuf(stockEvent);
                case AVRO -> StockValueFactory.avro(stockEvent);
                case JSON, JSON_SCHEMA -> StockValueFactory.json(stockEvent);
            };
        }

        /**
         * Resolves the {@link SerializationFormat} from the fully-qualified class name of the Kafka
         * value serializer.
         *
         * @param className the fully-qualified class name of the value serializer
         * @return the corresponding {@link SerializationFormat}
         * @throws IllegalArgumentException if {@code className} does not match any known serializer
         */
        public static SerializationFormat fromString(String className) {
            return switch (className) {
                case "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer",
                                "com.lightstreamer.kafka.examples.quick_start.producer.protobuf.CustomProtobufSerializer" ->
                        PROTOBUF;
                case "io.confluent.kafka.serializers.KafkaJsonSerializer" -> JSON;
                case "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
                                "com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer" ->
                        JSON_SCHEMA;
                case "io.confluent.kafka.serializers.KafkaAvroSerializer",
                                "com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer" ->
                        AVRO;

                default ->
                        throw new IllegalArgumentException(
                                "Unknown serializer class: " + className);
            };
        }
    }

    @Option(
            names = "--bootstrap-servers",
            description = "The Kafka connection string",
            required = false)
    String bootstrapServers;

    @Option(names = "--topic", description = "The target topic", required = true)
    String topic;

    @Option(
            names = "--config-file",
            description = "Optional producer config file",
            required = false)
    String configFile;

    /**
     * The Kafka producer instance used to send messages to Kafka topics. This producer uses Integer
     * as the key type and Object as the value type.
     */
    private KafkaProducer<Integer, Object> producer;

    /** The serialization format to be used by this producer when sending messages to Kafka. */
    private SerializationFormat serializationFormat;

    public Producer() {}

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
        this.serializationFormat = configure(configs);

        // Create the producer
        this.producer = new KafkaProducer<>(configs);

        // Create and start the feed simulator
        FeedSimulator simulator = new FeedSimulator(this);
        simulator.start();
    }

    @Override
    public void onEvent(int stockIndex, Map<String, String> stockEvent) {
        System.out.printf(
                "Sending event for stock %d on topic '%s': %s%n",
                stockIndex, this.topic, stockEvent.toString());
        ProducerRecord<Integer, Object> record =
                new ProducerRecord<>(
                        this.topic, stockIndex, serializationFormat.toValue(stockEvent));
        try {
            producer.send(
                    record,
                    (RecordMetadata metadata, Exception e) -> {
                        if (e != null) {
                            System.err.printf("Send failed:%n%s", e.getMessage());
                            return;
                        }
                        System.out.printf(
                                """
                                            Sent record:
                                            [
                                                KEY   => %d,
                                                VALUE => {%s}
                                            ]%n
                                            """,
                                record.key(), record.value().toString());
                    });
        } catch (Exception e) {
            e.printStackTrace();
            System.err.printf("Error sending records: %s%n", e.getMessage());
        }
    }

    /**
     * Configures the Kafka Producer with necessary properties.
     *
     * <p>This method coordinates three focused configuration steps:
     *
     * <ol>
     *   <li>Sets default Kafka producer properties (bootstrap servers, key/value serializers)
     *   <li>Validates the value serializer and resolves the corresponding {@link
     *       SerializationFormat}
     *   <li>Injects Azure Schema Registry credentials when an Azure Schema Registry serializer is
     *       in use (JSON or Avro)
     * </ol>
     *
     * @param config the {@code Properties} object to be configured with Kafka producer settings
     * @return the {@link SerializationFormat} resolved from the finalised configuration
     * @throws IllegalArgumentException if the configured value serializer class is unknown or if
     *     required Azure credential properties are missing
     */
    SerializationFormat configure(Properties config) {
        setKafkaDefaults(config);
        // Validate the value serializer early: throws IllegalArgumentException for unknown values.
        SerializationFormat resolvedType =
                SerializationFormat.fromString(
                        config.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        configureAzureCredential(config);
        return resolvedType;
    }

    /**
     * Loads properties from the specified configuration file.
     *
     * <p>This method creates a new Properties object and attempts to populate it from the file path
     * stored in the {@code configFile} field if it's not null. If the file doesn't exist or cannot
     * be read, the program will terminate with an error message.
     *
     * @return a {@code Properties} object containing configuration properties loaded from the file,
     *     or an empty {@code Properties} object if no configuration file was specified
     * @implNote Terminates the JVM with {@code System.exit(-1)} if the specified file does not
     *     exist or cannot be read.
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

                // com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer expects
                // auto.register.schemas as a Boolean, not a String
                if (properties.containsKey("auto.register.schemas")) {
                    properties.put(
                            "auto.register.schemas",
                            Boolean.parseBoolean(properties.getProperty("auto.register.schemas")));
                }

                // Print loaded properties on standard output
                System.out.println("Loaded the following properties from " + configFile + ":");
                properties.forEach((key, value) -> System.out.println(key + "=" + value));
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        return properties;
    }

    /**
     * Sets the essential Kafka producer properties.
     *
     * <ul>
     *   <li>Key serializer - set to {@link IntegerSerializer}
     *   <li>Value serializer - defaults to {@link KafkaJsonSerializer} if not explicitly set
     * </ul>
     *
     * @param config the {@code Properties} object to populate
     * @throws IllegalArgumentException if {@code bootstrap.servers} is absent from both the
     *     command-line argument and the supplied configuration
     */
    private void setKafkaDefaults(Properties config) {
        // Set bootstrap servers from command-line argument if provided, otherwise rely on config
        // file or defaults
        if (bootstrapServers != null) {
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        // Validate that bootstrap servers is set either via command-line or config file
        if (config.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new IllegalArgumentException("Missing required property: 'bootstrap.servers'");
        }
        config.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        config.putIfAbsent(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    /**
     * Injects an Azure {@link TokenCredential} into the configuration when an Azure Schema Registry
     * serializer ({@code KafkaJsonSerializer} or {@code KafkaAvroSerializer}) is the configured
     * value serializer. Has no effect for any other serializer.
     *
     * <p>Reads {@code tenant.id}, {@code client.id}, and {@code client.secret} from the supplied
     * configuration to build the credential.
     *
     * @param config the {@code Properties} object to inspect and, if applicable, enrich with the
     *     credential
     * @throws IllegalArgumentException if an Azure Schema Registry serializer is in use and any of
     *     {@code tenant.id}, {@code client.id}, or {@code client.secret} are absent or blank
     */
    private void configureAzureCredential(Properties config) {
        String valueSerializer = config.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        if (!Set.of(
                        com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer.class
                                .getName(),
                        com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer.class
                                .getName())
                .contains(valueSerializer)) {
            return;
        }

        String tenantId = config.getProperty("tenant.id");
        String clientId = config.getProperty("client.id");
        String clientSecret = config.getProperty("client.secret");
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required Azure credential property: 'tenant.id'");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required Azure credential property: 'client.id'");
        }
        if (clientSecret == null || clientSecret.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required Azure credential property: 'client.secret'");
        }
        TokenCredential credential =
                new ClientSecretCredentialBuilder()
                        .tenantId(tenantId)
                        .clientId(clientId)
                        .clientSecret(clientSecret)
                        .build();
            config.put(AZURE_SCHEMA_REGISTRY_CREDENTIAL, credential);
    }

    public static void main(String[] args) {
        new CommandLine(new Producer()).execute(args);
    }
}
