
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
import com.lightstreamer.kafka.examples.quick_start.producer.json.JsonStock;
import com.lightstreamer.kafka.examples.quick_start.producer.protobuf.ProtobufStock;
import com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer;

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
 * <p>This producer works with a {@code FeedSimulator} to generate stock events and publish them to
 * the specified Kafka topic.
 *
 * @see FeedSimulator.ExternalFeedListener
 * @see KafkaProducer
 * @see SerializerType
 */
public class Producer implements Runnable, FeedSimulator.ExternalFeedListener {

    /**
     * Enum representing the different types of serializers that can be used for serializing data in
     * the Kafka producer. The serializer type is determined based on the class name of the value
     * serializer specified in the configuration.
     */
    enum SerializerType {
        PROTOBUF,
        JSON,
        JSON_SCHEMA;

        public static SerializerType fromString(String className) {
            return switch (className) {
                case "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer" -> PROTOBUF;
                case "com.lightstreamer.kafka.examples.quick_start.producer.protobuf.CustomProtobufSerializer" ->
                        PROTOBUF;
                case "io.confluent.kafka.serializers.KafkaJsonSerializer" -> JSON;
                case "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer" -> JSON_SCHEMA;
                case "com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer" ->
                        JSON_SCHEMA;
                default ->
                        throw new IllegalArgumentException(
                                "Unknown serializer class: " + className);
            };
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

    /**
     * The property key used by the Azure Schema Registry Kafka serializer to obtain credentials.
     */
    static final String AZURE_SCHEMA_REGISTRY_CREDENTIAL = "schema.registry.credential";

    /**
     * The Kafka producer instance used to send messages to Kafka topics. This producer uses Integer
     * as the key type and Object as the value type.
     */
    private KafkaProducer<Integer, Object> producer;

    /** The type of serializer to be used by this producer when sending messages to Kafka. */
    private SerializerType serializerType;

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
        this.serializerType = configure(configs);

        // Create the producer
        this.producer = new KafkaProducer<>(configs);

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
     * Configures the Kafka Producer with necessary properties.
     *
     * <p>This method coordinates three focused configuration steps:
     *
     * <ol>
     *   <li>Sets default Kafka producer properties (bootstrap servers, key/value serializers)
     *   <li>Validates the value serializer and resolves the corresponding {@link SerializerType}
     *   <li>Injects Azure Schema Registry credentials when the Azure JSON serializer is in use
     * </ol>
     *
     * @param config the Properties object to be configured with Kafka producer settings
     * @return the {@link SerializerType} resolved from the finalised configuration
     * @throws IllegalArgumentException if the configured value serializer class is unknown or if
     *     required Azure credential properties are missing
     */
    SerializerType configure(Properties config) {
        setKafkaDefaults(config);
        // Validate the value serializer early: throws IllegalArgumentException for unknown values.
        SerializerType resolvedType =
                SerializerType.fromString(
                        config.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        configureAzureCredential(config);
        return resolvedType;
    }

    /**
     * Sets the essential Kafka producer properties.
     *
     * <ul>
     *   <li>Bootstrap servers - the Kafka cluster address
     *   <li>Key serializer - set to {@link IntegerSerializer}
     *   <li>Value serializer - defaults to {@link KafkaJsonSerializer} if not explicitly set
     * </ul>
     *
     * @param config the Properties object to populate
     */
    private void setKafkaDefaults(Properties config) {
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        config.putIfAbsent(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    /**
     * Injects an Azure {@link TokenCredential} into the configuration when the Azure JSON
     * serializer is the configured value serializer.
     *
     * <p>Reads {@code tenant.id}, {@code client.id}, and {@code client.secret} from the supplied
     * configuration to build the credential.
     *
     * @param config the Properties object to inspect and, if applicable, enrich with the credential
     */
    private void configureAzureCredential(Properties config) {
        if (!config.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                .equals(
                        com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer.class
                                .getName())) {
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

    @Override
    public void onEvent(int stockIndex, Map<String, String> stockEvent) {
        System.out.printf(
                "Receiving event for stock %d on topic '%s': %s%n",
                stockIndex, this.topic, stockEvent.toString());
        ProducerRecord<Integer, Object> record =
                new ProducerRecord<>(this.topic, stockIndex, toRecord(stockEvent, serializerType));
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
                                                KEY   => %d,
                                                VALUE => {%s}
                                            ]%n
                                            """,
                                    record.key(), record.value().toString());
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
            System.err.printf("Error sending records: %s%n", e.getMessage());
            throw e;
        }
    }

    /**
     * Converts a stock event represented as a map into the appropriate record object based on the
     * current serializer type.
     *
     * @param stockEvent a map containing stock event data with string keys and values
     * @return a serialization-specific object representation of the stock event: {@link JsonStock}
     *     for JSON/JSON_SCHEMA serializers or {@link ProtobufStock} for PROTOBUF serializer
     */
    Object toRecord(Map<String, String> stockEvent, SerializerType serializerType) {
        return switch (serializerType) {
            case JSON, JSON_SCHEMA -> JsonStock.fromEvent(stockEvent);
            case PROTOBUF -> ProtobufStock.fromEvent(stockEvent);
        };
    }

    public static void main(String[] args) {
        new CommandLine(new Producer()).execute(args);
    }
}
