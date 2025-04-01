package com.lightstreamer.examples.kafkademo.producer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DemoPublisher {

    private static Logger logger = LogManager.getLogger("kafkademo-producer");

    private static String kconnString; 

    private static int pause_millis = 800;

    private static boolean go = true;

    private static Random random = new Random();

    private static Calendar calendar = Calendar.getInstance();

    private static String topicName;

    private static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");

    private static HashMap<String, Integer> flight_destination = new HashMap<String, Integer>();

    private static HashMap<String, String> flight_departure = new HashMap<String, String>();

    private static HashMap<String, Integer> flight_momentum = new HashMap<String, Integer>();

    private static HashMap<String, Integer> board_position = new HashMap<String, Integer>();

    private static LinkedList<Integer> avl_pos = new LinkedList<Integer>();

    private static LinkedList<String> to_delete = new LinkedList<String>();

    private static final AtomicInteger counter = new AtomicInteger(0);

    private static List<String> destinations = Stream.of( "Seoul (ICN)",
    "Atlanta (ATL)",
    "Boston (BOS)",
    "Phoenix (PHX)",
    "Detroit (DTW)",
    "San Francisco (SFO)",
    "Salt Lake City (SLC)",
    "Fort Lauderdale (FLL)",
    "Los Angeles (LAX)",
    "Seattle (SEA)",
    "Miami (MIA)",
    "Orlando (MCO)",
    "Charleston (CHS)",
    "West Palm Beach (PBI)",
    "Fort Myers (RSW)",
    "San Salvador (SAL)",
    "Tampa (TPA)",
    "Portland (PWM)",
    "London (LHR)",
    "Malpensa (MXP)").collect(Collectors.toList());

    private static List<String> status_desc = Stream.of("Scheduled - On-time",
    "Scheduled - Delayed",
    "En Route - On-time",
    "En Route - Delayed",
    "Landed - On-time",
    "Landed - Delayed",
    "Cancelled",
    "Deleted").collect(Collectors.toList());

    private static List<String> status_icon = Stream.of("\uD83C\uDFAB",
    "⌛",
    "\uD83D\uDEEB",
    "\uD83D\uDEEC️",
    "✅",
    "\uD83D\uDFE9",
    "\uD83D\uDED1",
    "\uD83D\uDED1").collect(Collectors.toList());

    private static void kafkaproducerloop() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", kconnString);
        props.put("linger.ms", 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaJsonSerializer.class);

        while (go) {
            try {
                Future<RecordMetadata> futurek;
                RecordMetadata rmtdta;

                calendar.setTime(sdf.parse("07:00"));

                Producer<String, FlightInfo> producer = new KafkaProducer<>(props);

                int countdown_to_end_of_day = 0;
                boolean fisrt = true;

                while (go) {
                    String key;
                    String command;

                    if ( countdown_to_end_of_day-- == 0 ) {
                        
                        if (!fisrt) {
                            // Cleanup all the keys
                            for (String fno : flight_momentum.keySet()) {
                                System.out.println("Key: " + fno + ". Value: " + flight_momentum.get(fno));

                                FlightInfo endup = new FlightInfo("", "", fno,  0, "", "");
                                endup.command = "DELETE";

                                futurek = producer.send(new ProducerRecord<String, FlightInfo>(topicName, fno, endup));

                                rmtdta = futurek.get();
                            }


                            logger.info("Send CS messasge.");

                            FlightInfo cs = new FlightInfo("", "", "",  0, "", "");
                            cs.command = "CS";

                            futurek = producer.send(new ProducerRecord<String, FlightInfo>(topicName, "snapshot", cs));

                            rmtdta = futurek.get();

                            logger.info("Sent message to partition: " + rmtdta.partition());
                        } else {
                            fisrt = false;
                        }
                        
                        flight_momentum.clear();
                        board_position.clear();
                        avl_pos.clear();

                        // snapshots
                        IntStream.range(0, 10).forEach(i -> {
                            int tmp = random.nextInt(900) + 10;

                            String k = "LS" + tmp;
                            String cmd = "ADD";

                            FlightInfo flightinfo = getrandominfo(k);
                            
                            logger.info("Key : " + k + ", new Message : " + flightinfo.departure);
    
                            flightinfo.currentTime = sdf.format(calendar.getTime());
                            flightinfo.command = cmd;
    
                            Future<RecordMetadata> wait = producer.send(new ProducerRecord<String, FlightInfo>(topicName, k, flightinfo));
    
                            try {
                                wait.get();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            }
    
                            logger.info("Sent snapshot.");
                        });

                        logger.info("Send EOS messasge.");

                        FlightInfo eos = new FlightInfo("", "", "",  0, "", "");
                        eos.command = "EOS";

                        futurek = producer.send(new ProducerRecord<String, FlightInfo>(topicName, "snapshot", eos));

                        rmtdta = futurek.get();

                        logger.info("Sent message to partition: " + rmtdta.partition());

                        countdown_to_end_of_day = 100;

                    } else {
                        if (flight_momentum.size() < 10) {
                            int tmp = random.nextInt(900) + 10;

                            key = "LS" + tmp;
                            command = "ADD";
                        } else {                           
                            key = getRandomKey(flight_momentum);

                            command = "UPDATE";
                        }

                        FlightInfo flightinfo = getrandominfo(key);
                        if (to_delete.size() > 0 ) {
                            command = "DELETE";
                            to_delete.clear();
                        }                         
                        logger.info("Key : " + key + ", new Message : " + flightinfo.departure);

                        flightinfo.currentTime = sdf.format(calendar.getTime());
                        flightinfo.command = command;

                        futurek = producer.send(new ProducerRecord<String, FlightInfo>(topicName, key, flightinfo));

                        rmtdta = futurek.get();

                        logger.info("Sent message to partition: " + rmtdta.partition());
                    }

                    Thread.sleep(pause_millis);
                }

                producer.close();

            } catch (Exception e) {
                logger.error("Error during producer loop: " + e.getMessage());
            }

            Thread.sleep(2500);
        }
    }

    private static int nextFlightStatus(int from) {
        int to = from;

        if (from == 0) {
            if ( random.nextBoolean() ) {
                to = 2;
            } else {
                to = 1;
            }
        } else if (from == 1) {
            if ( random.nextBoolean() ) {
                to = 3;
            } else {
                to = 6;
            }
        } else if (from == 2) {
            if ( random.nextBoolean() ) {
                to = 3;
            } else {
                to = 4;
            }
        } else if (from == 3) {
            to = 5;
        } else {
            // The Flight should be removed by the departures board
            to = 7;
        }

        return to;
    }

    private static FlightInfo getrandominfo(String key) {
        FlightInfo flightinfo;

        int inds;
        int indx;

        String departure;

        if (flight_momentum.containsKey(key)) {
            inds = nextFlightStatus(flight_momentum.get(key).intValue());
            indx = flight_destination.get(key);
            departure = flight_departure.get(key);
        } else {
            inds = 0;

            indx = random.nextInt(20);
            flight_destination.put(key, indx);

            calendar.add(Calendar.MINUTE, 3);
            departure = sdf.format(calendar.getTime());
            flight_departure.put(key, departure);
        }

        if (inds == 7) {
            inds = flight_momentum.remove(key);

            Integer removed = board_position.remove(key);
            to_delete.add(key);
            if (removed != null) {
                avl_pos.add(removed);
            }
        } else {
            flight_momentum.put(key, Integer.valueOf(inds));

            if (!board_position.containsKey(key)) {
                if (avl_pos.size() > 0) {
                    board_position.put(key, avl_pos.remove(0));
                } else {
                    int pos = counter.incrementAndGet();
                    board_position.put(key, Integer.valueOf(pos));
                }
            }
        }

        int trmnl = random.nextBoolean() ? 3 : 7;

        if ((inds == 1) || (inds == 3)) {
            calendar.add(Calendar.MINUTE, 3);
            departure = sdf.format(calendar.getTime());

            flight_departure.put(key, departure);
        }

        flightinfo = new FlightInfo(status_icon.get(inds) + ' ' + destinations.get(indx), departure, key, trmnl,
            status_desc.get(inds), "Lightstreamer Airlines");

        return flightinfo;
    }

    private static String getRandomKey(Map<String, Integer> map) {
        Set<String> keys = map.keySet();
        String[] keyArray = keys.toArray(new String[0]);

        Random random = new Random();
        int randomIndex = random.nextInt(keyArray.length);

        return keyArray[randomIndex];
    }

    public static void main(String[] args) {
        
        logger.info("Start Kafka demo producer: " + args.length);

        if (args.length < 3) {
            logger.error("Missing arguments: [bootstrap-servers] [topic-name] [interval of update]");
            return ;
        }

        kconnString = args[0];
        topicName = args[1];

        try {
            pause_millis = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            logger.error("Interval of update not valid assumd 800ms.");
            pause_millis = 800;
        }
        logger.info("wait pause in millis : " + pause_millis);

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    kafkaproducerloop();
                } catch (InterruptedException e) {
                    logger.error("Unexpected error running the producer loop: " + e.getMessage());
                }
            }
        });  
        t1.start();
        
        // String input = System.console().readLine();
        // while (!input.equalsIgnoreCase("stop")) {
        // input = System.console().readLine();
        // }

        // go = false;

        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            // ...
        }

        logger.info("End Kafka demo producer.");
    }
}
