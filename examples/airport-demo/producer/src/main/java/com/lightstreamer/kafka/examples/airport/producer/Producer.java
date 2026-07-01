
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.examples.airport.producer;

import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.CANCELLED;
import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.DELETED;
import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.EN_ROUTE_DELAYED;
import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.EN_ROUTE_ON_TIME;
import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.LANDED_DELAYED;
import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.LANDED_ON_TIME;
import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.SCHEDULED_DELAYED;
import static com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus.SCHEDULED_ON_TIME;

import com.lightstreamer.kafka.examples.airport.producer.FlightInfo.FlightStatus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private static final int MAX_BOARD_ROWS = 10;

    private static final int MIN_FLIGHT_NUMBER = 100;

    private static final int MAX_FLIGHT_NUMBER = 999;

    private static final int MAX_DEPARTURE_DELAY_MINUTES = 60;

    private static final String FLIGHT_PREFIX = "LS";

    private static final List<String> TERMINALS = List.of("1", "2", "3");

    private static final List<String> AIRLINES =
            List.of("Lightstreamer Airlines", "Air Kafka", "Topic Airways");

    private static final List<String> DESTINATIONS =
            List.of(
                    "Seoul (ICN)",
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
                    "Malpensa (MXP)");

    private static final long RANDOM_SEED = 42L;

    private static final String USAGE =
            "Usage: Producer <bootstrap-servers> <topic-name> <max-pause-millis>";

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm");

    private final String topicName;

    private final int maxPauseMillis;

    private final Random random = new Random(RANDOM_SEED);

    private final Map<String, FlightInfo> flights = new HashMap<>();

    private final KafkaProducer<String, FlightInfo> producer;

    private volatile boolean running = true;

    Producer(String bootstrapServers, String topicName, int maxPauseMillis) {
        this.topicName = topicName;
        this.maxPauseMillis = maxPauseMillis;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaJsonSerializer.class);
        this.producer = new KafkaProducer<>(props);
    }

    private FlightInfo pickFlight() {
        FlightInfo flightInfo;
        if (flights.size() < MAX_BOARD_ROWS) {
            String flightNumber = newFlightNumber();
            String destination = DESTINATIONS.get(random.nextInt(DESTINATIONS.size()));

            LocalTime now = LocalTime.now();
            String scheduledTime = now.format(TIME_FORMAT);
            FlightStatus initialStatus =
                    random.nextBoolean() ? SCHEDULED_ON_TIME : SCHEDULED_DELAYED;
            flightInfo =
                    new FlightInfo(
                            flightNumber,
                            destination,
                            TERMINALS.get(random.nextInt(TERMINALS.size())),
                            AIRLINES.get(random.nextInt(AIRLINES.size())),
                            scheduledTime,
                            effectiveTime(initialStatus, scheduledTime),
                            initialStatus);
            flights.put(flightNumber, flightInfo);
        } else {
            flightInfo = randomExistingFlight();
            flightInfo.setStatus(nextFlightStatus(flightInfo.getStatus()));
            flightInfo.setEffective(
                    effectiveTime(flightInfo.getStatus(), flightInfo.getScheduled()));
        }

        if (flightInfo.getStatus() == DELETED) {
            flights.remove(flightInfo.getFlightNo());
        }

        return flightInfo;
    }

    private String effectiveTime(FlightStatus status, String scheduled) {
        if (status == SCHEDULED_DELAYED) {
            LocalTime time = LocalTime.parse(scheduled, TIME_FORMAT);
            return time.plusMinutes(random.nextInt(MAX_DEPARTURE_DELAY_MINUTES))
                    .format(TIME_FORMAT);
        }
        return scheduled;
    }

    private FlightStatus nextFlightStatus(FlightStatus from) {
        return switch (from) {
            case SCHEDULED_ON_TIME -> random.nextBoolean() ? EN_ROUTE_ON_TIME : SCHEDULED_DELAYED;
            case SCHEDULED_DELAYED -> random.nextBoolean() ? EN_ROUTE_ON_TIME : CANCELLED;
            case EN_ROUTE_ON_TIME -> random.nextBoolean() ? EN_ROUTE_DELAYED : LANDED_ON_TIME;
            case EN_ROUTE_DELAYED -> LANDED_DELAYED;
            case CANCELLED, LANDED_ON_TIME, LANDED_DELAYED -> DELETED;
            case DELETED -> throw new IllegalStateException("Cannot transition from DELETED");
        };
    }

    private String newFlightNumber() {
        String key;
        do {
            key =
                    FLIGHT_PREFIX
                            + (random.nextInt(MAX_FLIGHT_NUMBER - MIN_FLIGHT_NUMBER + 1)
                                    + MIN_FLIGHT_NUMBER);
        } while (flights.containsKey(key));
        return key;
    }

    private FlightInfo randomExistingFlight() {
        FlightInfo[] values = flights.values().toArray(new FlightInfo[0]);
        return values[random.nextInt(values.length)];
    }

    private void loop() {
        while (running) {
            try {
                publish(pickFlight());
                TimeUnit.MILLISECONDS.sleep(random.nextInt(maxPauseMillis));
            } catch (Exception e) {
                logger.atError().setCause(e).log("Error during producer loop");
            }
        }
        producer.close();
    }

    private void shutdown() {
        running = false;
    }

    private void publish(FlightInfo flight) {
        FlightInfo value = flight.getStatus() == DELETED ? null : flight;
        producer.send(
                new ProducerRecord<>(topicName, flight.getFlightNo(), value),
                (recordMetadata, e) -> {
                    if (e != null) {
                        logger.atError().setCause(e).log("Error sending message");
                        return;
                    }
                    logger.atInfo().log(
                            "Sent key {}, destination: {}, status: {}",
                            flight.getFlightNo(),
                            flight.getDestinationStr(),
                            flight.getStatus());
                });
    }

    public static void main(String[] args) {
        if (args.length == 1 && ("--help".equals(args[0]) || "-h".equals(args[0]))) {
            System.out.println(USAGE);
            return;
        }
        if (args.length < 3) {
            logger.atError().log("Missing arguments. {}", USAGE);
            return;
        }

        String bootstrapServers = args[0];
        String topicName = args[1];

        int maxPauseMillis;
        try {
            maxPauseMillis = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            logger.atError()
                    .setCause(e)
                    .log("Max pause millis is not a valid integer: {}", args[2]);
            return;
        }
        if (maxPauseMillis <= 0) {
            logger.atError().log("Max pause millis must be positive, got: {}", maxPauseMillis);
            return;
        }
        logger.atInfo().log(
                "Start Airport Demo producer: bootstrap-servers={}, topic={}, max-pause={}ms",
                bootstrapServers,
                topicName,
                maxPauseMillis);

        Producer producer = new Producer(bootstrapServers, topicName, maxPauseMillis);
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    logger.atInfo().log("Shutdown signal received.");
                                    producer.shutdown();
                                    try {
                                        mainThread.join();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }));
        producer.loop();
        logger.atInfo().log("End Airport Demo producer.");
    }
}
