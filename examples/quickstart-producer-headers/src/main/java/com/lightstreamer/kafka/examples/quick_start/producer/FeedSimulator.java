
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simulator for generating financial position data for multiple accounts in a controlled,
 * real-time environment. This class creates simulated market position events with randomized but
 * statistically consistent timing and values.
 *
 * <p>The simulator generates position events for a predefined set of accounts, with each position
 * having attributes such as account ID, position ID, direction (buy/sell), instrument name, and
 * price. The generated events are delivered to a registered listener at intervals determined by
 * configurable normal distributions, simulating the variable timing of real market data feeds.
 *
 * <p>For each account, the simulator maintains up to 10 positions that are either created as new or
 * updated with new direction and price values. This simulates the dynamic nature of a real trading
 * system where positions are opened, modified, and closed over time.
 *
 * <p>The simulation runs on a thread pool to ensure timely delivery of events without blocking the
 * main application thread. Events are generated on separate scheduled tasks for each account.
 *
 * <p>Usage example:
 *
 * <pre>
 * ExternalFeedListener listener = event -> {
 *     // Process position event
 * };
 * FeedSimulator simulator = new FeedSimulator(listener);
 * simulator.start();
 * </pre>
 *
 * <p>This class is designed for testing and demonstration purposes, particularly for applications
 * that need to process streaming financial data, such as the Kafka connector examples.
 */
public class FeedSimulator {

    /**
     * Represents a trading position.
     *
     * <p>All fields are annotated with {@link JsonProperty} for JSON serialization/deserialization.
     *
     * @param accountId the identifier of the account owning this position
     * @param positionId the unique identifier for this position
     * @param direction the trading direction (e.g., "long", "short")
     * @param instrument the financial instrument being traded
     * @param open the opening price of the position
     */
    record Position(
            @JsonProperty String accountId,
            @JsonProperty String positionId,
            @JsonProperty String direction,
            @JsonProperty String instrument,
            @JsonProperty double open) {

        Position newPosition(String direction, double newPrice) {
            return new Position(accountId(), positionId(), direction, instrument, newPrice);
        }
    }

    /** Defines a listener interface for receiving position events from an external feed. */
    public interface ExternalFeedListener {

        /**
         * Called when a position event occurs.
         *
         * @param event the Position event to be processed
         */
        void onEvent(Position event);
    }

    /**
     * An array of account identifiers used in the feed simulator. These identifiers are used to
     * simulate different accounts for which stock data is generated. Each identifier corresponds to
     * a specific account in the simulated market data.
     */
    private static final String[] ACCOUNT_IDS = {
        "F33455D", "F33455E", "F33455F", "F33455G", "F33455H", "F33455I", "F33455J", "F33455K",
        "F33455L", "F33455M"
    };

    private static final String[] DIRECTIONS = {"buy", "sell"};

    /**
     * Array of mean time values (in milliseconds) between updates for simulated data items. Each
     * element represents the average delay between consecutive updates for a specific item in the
     * feed simulation. Different values create varied update frequencies for different items,
     * producing a more realistic data simulation pattern.
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

    /**
     * Array of standard deviations (in milliseconds) for update time intervals. These values are
     * used to generate randomized update intervals following normal distributions when simulating
     * the feed data. Each entry represents the standard deviation for a specific item or data
     * point, allowing for variable timing patterns across different simulated feeds.
     */
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

    /**
     * Array of fictional company names used to simulate stock data in the feed simulator. These
     * names represent mock stocks for testing and demonstration purposes within the Kafka connector
     * example. Each element represents a company name that will be used when generating simulated
     * market data events.
     */
    private static final String[] INSTRUMENT_NAMES = {
        "Anduct", "Ations Europe", "Bagies Consulting", "BAY Corporation",
        "CON Consulting", "Corcor PLC", "CVS Asia", "Datio PLC",
        "Dentems", "ELE Manufacturing", "Exacktum Systems", "KLA Systems Inc",
        "Lted Europe", "Magasconall Capital", "MED", "Mice Investments",
        "Micropline PLC", "Nologicroup Devices", "Phing Technology", "Pres Partners",
        "Quips Devices", "Ress Devices", "Sacle Research", "Seaging Devices",
        "Sems Systems, Inc", "Softwora Consulting", "Systeria Develop", "Thewlec Asia",
        "Virtutis", "Yahl"
    };

    /** The listener that will be notified of events from the external feed. */
    private final ExternalFeedListener listener;

    /**
     * Executor service responsible for scheduling periodic feed simulation tasks. This scheduler
     * manages the timing of data generation and ensures the feed simulation runs at configured
     * intervals.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Constructs a new FeedSimulator with the specified listener. Initializes a scheduled thread
     * pool with 4 threads for executing feed simulation tasks.
     *
     * @param listener The listener that will receive notifications about feed events
     */
    public FeedSimulator(ExternalFeedListener listener) {
        this.listener = listener;
        this.scheduler = Executors.newScheduledThreadPool(4);
    }

    /**
     * Starts the feed simulation by initializing and scheduling position producers for each
     * account. This method creates a shared PositionIdProvider and then for each account ID:
     *
     * <ul>
     *   <li>Creates a new {@link PositionsProducer}
     *   <li>Schedules it for immediate execution (with zero wait time)
     * </ul>
     *
     * Once started, the feed simulator will begin generating position data for all configured
     * accounts.
     */
    public void start() {
        PositionIdProvider positionIdProvider = new PositionIdProvider();
        for (int i = 0; i < ACCOUNT_IDS.length; i++) {
            PositionsProducer position = new PositionsProducer(i, positionIdProvider);
            long waitTime = 0;
            scheduleGenerator(position, waitTime);
        }
    }

    private void scheduleGenerator(PositionsProducer producer, long waitTime) {
        scheduler.schedule(
                () -> {
                    long nextWaitTime;
                    Position eventData;
                    synchronized (producer) {
                        eventData = producer.getPosition();
                        nextWaitTime = producer.computeNextWaitTime();
                    }
                    listener.onEvent(eventData);
                    scheduleGenerator(producer, nextWaitTime);
                },
                waitTime,
                TimeUnit.MILLISECONDS);
    }

    /** Provides unique position identifiers for simulated stock positions. */
    private static class PositionIdProvider {

        /**
         * A prefix used to generate position identifiers for the simulated stocks. Each stock will
         * have a unique position identifier that starts with this prefix followed by a numeric
         * index.
         */
        private static final String POSITION_ID_PREFIX = "FX";

        private final AtomicInteger positionIdCounter = new AtomicInteger(0);

        /**
         * Generates a unique position ID by incrementing a counter and formatting it with a prefix.
         *
         * @return a unique position ID string
         */
        public String generatePositionId() {
            return String.format("%s%d", POSITION_ID_PREFIX, positionIdCounter.incrementAndGet());
        }
    }

    /**
     * Simulates the production of trading positions for a specific account.
     *
     * <p>This class generates random trading position data including position IDs, directions
     * (buy/sell), instruments, and prices. It maintains a collection of positions up to a maximum
     * limit ({@value #MAX_POSITIONS}), and alternates between creating new positions and updating
     * existing ones.
     *
     * <p>Each instance is associated with a specific account and uses predefined parameters (mean
     * and standard deviation) to control the timing and randomness of position updates. The
     * Gaussian distribution is used to create realistic time intervals between position updates.
     *
     * <p>This class is designed to be used within a feed simulator to generate realistic trading
     * activity for testing or demonstration purposes.
     */
    private static class PositionsProducer {

        private static final int MAX_POSITIONS = 10;

        /** The unique identifier for this feed simulator instance */
        private final int accountIdIndex;

        /** The mean price of the stock, used for generating random price changes */
        private final double mean;

        /** The standard deviation of the stock price, used for generating random price changes */
        private final double stdDev;

        /**
         * Random number generator used to produce simulated data values for the feed. This instance
         * is used across the simulator to ensure consistent random value generation when creating
         * mock data points.
         */
        private final Random random = new Random();

        /** Provider for generating unique position identifiers. */
        private final PositionIdProvider positionIdProvider;

        private List<Position> currentPositions = new ArrayList<>();

        PositionsProducer(int accountIdIndex, PositionIdProvider positionIdProvider) {
            if (accountIdIndex < 0 || accountIdIndex >= ACCOUNT_IDS.length) {
                throw new IllegalArgumentException(
                        "Index out of bounds for predefined arrays: " + accountIdIndex);
            }
            this.positionIdProvider = positionIdProvider;
            this.accountIdIndex = accountIdIndex;
            this.mean = UPDATE_TIME_MEANS[accountIdIndex];
            this.stdDev = UPDATE_TIME_STD_DEVS[accountIdIndex];
        }

        /**
         * Computes the next wait time using a Gaussian distribution.
         *
         * <p>This method generates a random wait time in milliseconds based on a normal
         * distribution with the specified mean and standard deviation. If the generated value is
         * negative or zero, it will keep generating values until a positive value is obtained.
         *
         * @return a positive wait time in milliseconds
         */
        public long computeNextWaitTime() {
            long millis;
            do {
                double base = random.nextGaussian();
                millis = (long) (base * this.stdDev + mean);
            } while (millis <= 0);
            return millis;
        }

        String positionId() {
            return positionIdProvider.generatePositionId();
        }

        String direction() {
            return DIRECTIONS[random.nextInt(2)];
        }

        String instrument() {
            return INSTRUMENT_NAMES[random.nextInt(INSTRUMENT_NAMES.length)];
        }

        double price() {
            return random.nextDouble(4) + 0.1;
        }

        Position getPosition() {
            if (currentPositions.size() < MAX_POSITIONS) {
                return addAndGetPosition(currentPositions);
            }
            return updatePosition(currentPositions);
        }

        private Position updatePosition(List<Position> positions) {
            // Get a random position from the list and update it
            // to simulate a new position with the same accountId and instrument
            // but a different direction and price.
            int positionIndex = random.nextInt(positions.size());
            Position currentPosition = positions.get(positionIndex);
            Position updatedPosition = currentPosition.newPosition(direction(), price());
            // Update the position in the list
            positions.set(positionIndex, updatedPosition);
            return updatedPosition;
        }

        private Position addAndGetPosition(List<Position> positions) {
            Position newPosition =
                    new Position(
                            ACCOUNT_IDS[accountIdIndex],
                            positionId(),
                            direction(),
                            instrument(),
                            price());
            positions.add(newPosition);
            return newPosition;
        }
    }
}
