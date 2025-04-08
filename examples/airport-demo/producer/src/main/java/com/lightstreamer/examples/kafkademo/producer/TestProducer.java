package com.lightstreamer.examples.kafkademo.producer;


    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerRecord;
    import org.apache.kafka.clients.producer.ProducerConfig;
    import org.apache.kafka.common.serialization.StringSerializer;
    
    import com.google.gson.JsonObject;
    
    import java.util.Properties;
    import java.util.Random;
    import java.util.Timer;
    import java.util.TimerTask;

    public class TestProducer {
    
        public static void main(String[] args) {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    
            String topic = "test"; 
            Random random = new Random();
    
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    JsonObject message = new JsonObject();
                    message.addProperty("type", "sub");
    
                    JsonObject data = new JsonObject();
                    String randomStock = "[SAMA]" + (1000 + random.nextInt(100));
                    data.addProperty("subject", "-/stocks/quote/" + randomStock);
    
                    message.add("data", data);
    
                    String jsonString = message.toString();
    
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonString);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Error: " + exception.getMessage());
                        } else {
                            System.out.println("Msg: " + jsonString);
                        }
                    });
                }
            }, 0, 2000); 
    
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Chiusura del producer...");
                producer.close();
                timer.cancel();
            }));
        }
    }
    
