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

import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Simulates an external data feed that supplies quote values for all the stocks
 * needed for the
 * demo.
 */
public class FeedSimulator {

    public interface ExternalFeedListener {

        void onEvent(String topic, String symbol, String data);
    }

    enum FIELD {
        NAME,
        TRow,
        TMSTMP,
        QCHART,
        QCHARTTOT,
        VTOT,
        QTOT,
        QA,
        Q,
        PA,
        PCHART,
        NTRAD,
        PLIM_MIN,
        PLIM_MAX
    }

    /**
     * Used to automatically generate the updates for the 30 stocks: mean and
     * standard deviation of the times between consecutive updates for the same
     * stock.
     */
    private static final double[] UPDATE_TIME_MEANS = {
            30000, 500, 3000, 90000,
            7000, 10000, 3000, 7000,
            7000, 7000, 500, 3000,
            20000, 20000, 20000, 30000,
            500, 3000, 90000, 7000,
            10000, 3000, 7000, 7000,
            7000, 500, 3000, 20000,
            20000, 20000,
    };

    private static final double[] UPDATE_TIME_STD_DEVS = {
            6000, 300, 1000, 1000,
            100, 5000, 1000, 3000,
            1000, 6000, 300, 1000,
            1000, 4000, 1000, 6000,
            300, 1000, 1000, 100,
            5000, 1000, 3000, 1000,
            6000, 300, 1000, 1000,
            4000, 1000,
    };

    private static final String[] SYMBOL_NAMES = {
            "Anduct", "Ations Europe",
            "Bagies Consulting", "BAY Corporation",
            "CON Consulting", "Corcor PLC",
            "CVS Asia", "Datio PLC",
            "Dentems", "ELE Manufacturing",
            "Exacktum Systems", "KLA Systems Inc",
            "Lted Europe", "Magasconall Capital",
            "MED", "Mice Investments",
            "Micropline PLC", "Nologicroup Devices",
            "Phing Technology", "Pres Partners",
            "Quips Devices", "Ress Devices",
            "Sacle Research", "Seaging Devices",
            "Sems Systems, Inc", "Softwora Consulting",
            "Systeria Develop", "Thewlec Asia",
            "Virtutis", "Yahl"
    };

    private static String TOPIC_PREFIX = "MPDF.SIT.DATA.QUOTE";

    private static final String[] TOPICS = {
            "SAMA.1010", "SAMA.1020",
            "SAMA.1030", "SAMA.1040",
            "SAMA.1050", "SAMA.1060",
            "SAMA.1070", "SAMA.1080",
            "SAMA.1090", "SAMA.1100",
    };

    /** Manages the current state and generates update events for a single stock. */
    private static class QuoteProducer {

        private static final DateTimeFormatter TMSTP_FORMATTER = DateTimeFormatter.ofPattern("YYYY-MM-ddHH:mm:ss");
        private static final Random random = new SecureRandom();

        private final double mean, stddev;
        private final String name;
        private String targetTopic;

        /** Initializes stock data based on the already prepared values. */
        public QuoteProducer(int itemPos) {
            this.name = SYMBOL_NAMES[itemPos % TOPICS.length];
            this.targetTopic = formatTopic(itemPos % TOPICS.length); // A topic hosts only one symbol
            this.mean = UPDATE_TIME_MEANS[itemPos % UPDATE_TIME_MEANS.length];
            this.stddev = UPDATE_TIME_STD_DEVS[itemPos % UPDATE_TIME_STD_DEVS.length];
        }

        public String getName() {
            return name;
        }

        public String getTopic() {
            return targetTopic;
        }

        /**
         * Decides, for ease of simulation, the time at which the next update for the
         * stock will happen.
         */
        public long computeNextWaitTime() {
            long millis;
            do {
                millis = gaussian(mean, stddev);
            } while (millis <= 0);
            return millis;
        }

        public String formatTopic(int topicIndex) {
            return "%s.%s".formatted(TOPIC_PREFIX, TOPICS[topicIndex]);
        }

        public Map<FIELD, String> getCurrentValues() {
            EnumMap<FIELD, String> event = new EnumMap<>(FIELD.class);
            event.put(FIELD.NAME, name);
            event.put(FIELD.TRow, String.valueOf(random.nextInt(1000, 200000)));
            event.put(FIELD.TMSTMP, LocalDateTime.now().format(TMSTP_FORMATTER));
            event.put(FIELD.QCHARTTOT, String.valueOf(random.nextInt(2000, 2040)));
            event.put(FIELD.QCHART, String.valueOf(random.nextInt(1, 2)));
            event.put(FIELD.VTOT, String.valueOf(random.nextInt(8000, 81316)));
            event.put(FIELD.QTOT, String.valueOf(random.nextInt(2000, 2040)));
            event.put(FIELD.Q, String.valueOf(random.nextInt(1, 2)));
            event.put(FIELD.QA, String.valueOf(random.nextInt(9012, 9050)));
            event.put(FIELD.PA, String.valueOf(random.nextInt(30, 45)));
            event.put(FIELD.PCHART, String.valueOf(random.nextInt(40, 50)));
            event.put(FIELD.NTRAD, String.valueOf(random.nextInt(100, 106)));
            event.put(FIELD.PLIM_MIN, String.valueOf(random.nextInt(9, 28)));
            event.put(FIELD.PLIM_MAX, String.valueOf(random.nextInt(50, 70)));
            return event;
        }

        private long gaussian(double mean, double stddev) {
            double base = random.nextGaussian();
            return (long) (base * stddev + mean);
        }
    }

    /** Used to keep the contexts of the 30 stocks. */
    private final List<QuoteProducer> stockGenerators = new ArrayList<>();

    /** The internal listener for the update events. */
    private final ExternalFeedListener listener;

    private ScheduledExecutorService scheduler;

    public FeedSimulator(ExternalFeedListener listener) {
        this.listener = listener;
        this.scheduler = Executors.newScheduledThreadPool(4);
    }

    /**
     * Starts generating update events for the stocks. Simulates attaching and
     * reading from an external broadcast feed.
     */
    public void start() {
        for (int i = 0; i < TOPICS.length; i++) {
            QuoteProducer quote = new QuoteProducer(i);
            stockGenerators.add(quote);
            long waitTime = 0; // stock.computeNextWaitTime();
            scheduleGenerator(quote, waitTime);
        }
    }

    /**
     * Generates new values and sends a new update event at the time the producer
     * declared to do it.
     */
    private void scheduleGenerator(QuoteProducer quoteProducer, long waitTime) {
        scheduler.schedule(
                () -> {
                    long nextWaitTime;
                    synchronized (quoteProducer) {
                        listener.onEvent(
                                quoteProducer.getTopic(), quoteProducer.getName(),
                                formatEvent(quoteProducer.getCurrentValues()));

                        nextWaitTime = quoteProducer.computeNextWaitTime();
                    }
                    scheduleGenerator(quoteProducer, nextWaitTime);
                },
                waitTime,
                TimeUnit.MILLISECONDS);
    }

    static String formatEvent(Map<FIELD, String> event) {
        return event.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
    }

    public static void main(String[] args) {
        FeedSimulator feedSimulator = new FeedSimulator(
                new ExternalFeedListener() {

                    @Override
                    public void onEvent(String topic, String symbol, String quote) {
                        System.out.println("topic: " + topic + ", symbol: " + symbol + ", quote: " + quote);
                    }
                });
        feedSimulator.start();
    }
}
