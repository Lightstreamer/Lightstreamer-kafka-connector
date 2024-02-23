

import java.security.SecureRandom;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Producer implements Runnable {

    @Option(
            names = "--bootstrap-servers",
            description = "The Kafka connection string",
            required = true)
    private String bootstrapServers;

    @Option(names = "--topic", description = "The target topic", required = true)
    private String topic;

    @Option(names = "--period", description = "The interval in ms between two successive executions", required = false, defaultValue="50")
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

        // Create and start producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties);) {
            while (true) {
                String message =
                new SecureRandom()
                        .ints(20, 48, 122)
                        .mapToObj(Character::toString)
                        .collect(Collectors.joining());

                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(this.topic, message);
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
