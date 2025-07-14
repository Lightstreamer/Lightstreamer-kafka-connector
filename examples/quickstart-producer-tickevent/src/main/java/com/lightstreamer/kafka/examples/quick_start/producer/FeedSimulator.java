
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

import com.example.tutorial.protos.BidAsk;
import com.example.tutorial.protos.Tick;
import com.example.tutorial.protos.TickEvent;
import com.google.protobuf.Timestamp;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A simulator for generating synthetic crypto data feeds.
 *
 * <p>This class simulates a real-time financial data feed by generating realistic price movements
 * and other market metrics for a collection of fictional crypto. It's designed to produce a
 * continuous stream of market events with variable timing and non-divergent price walks, mimicking
 * the behavior of an external financial data broadcast feed.
 *
 * <p>The simulator:
 *
 * <ul>
 *   <li>Generates data for 10 different simulated cryptos
 *   <li>Produces realistic price movements that tend to revert to reference prices
 *   <li>Uses Gaussian distributions to create natural-seeming update intervals
 *   <li>Delivers events through a listener pattern to registered clients
 * </ul>
 *
 * <p>Each simulated crypto includes:
 *
 * <ul>
 *   <li>Symbol name
 *   <li>Price information (bid, ask)
 *   <li>Timestamps
 * </ul>
 *
 * <p>The simulation runs on a scheduled thread pool and will continue producing events until
 * explicitly stopped or the application terminates.
 */
public class FeedSimulator {

    /**
     * A listener interface for receiving events from an external feed simulator. Implementations of
     * this interface can be registered with a feed simulator to be notified when new crypto events
     * are generated.
     */
    public interface ExternalFeedListener {

        /**
         * Callback method invoked when a new {@link TickEvent} occurs.
         *
         * @param tickEvent the event containing tick data to be processed
         */
        void onEvent(TickEvent tickEvent);
    }

    /**
     * Used to automatically generate the updates for the 10 cryptos: mean and standard deviation of
     * the times between consecutive updates for the same crypto.
     */
    private static final double[] UPDATE_TIME_MEANS = {
        30000, 500, 3000, 90000, 7000,
        500, 3000, 90000, 7000, 10000
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
        1000, 6000
    };

    /**
     * Array of reference crypto prices used for simulating market data. These values serve as base
     * prices for generating realistic price movements in the simulated feed. Each value represents
     * a different crypto's reference price in currency units (likely USD).
     */
    private static final double[] REF_PRICES = {
        3.04, 16.09, 7.19, 3.63, 7.61, 2.30, 15.39, 5.31, 4.86, 7.61
    };

    /**
     * Array of fixed opening prices used for market data simulation. Each value represents the
     * initial price point for a different financial instrument in the simulation. These values are
     * used as the base prices for generating market data fluctuations in the feed simulator.
     */
    private static final double[] OPEN_PRICES = {
        3.10, 16.20, 7.25, 3.62, 7.65, 2.30, 15.85, 5.31, 4.97, 7.70
    };

    /** Array of symbols used to simulate crypto data in the feed simulator. */
    private static final String[] SYMBOL_NAMES = {
        "ETH", "BTH", "LTH", "XRP", "BNB", "TRX", "XLM", "SUI", "TON", "SOL"
    };

    /**
     * Listener interface implementation that gets notified of updates from this feed simulator. The
     * listener handles the processing of simulated data events for external consumption.
     */
    private final ExternalFeedListener listener;

    /**
     * Executor service responsible for scheduling periodic feed simulation tasks. This scheduler
     * manages the timing of data generation and ensures the feed simulation runs at configured
     * intervals.
     */
    private ScheduledExecutorService scheduler;

    /**
     * Constructs a FeedSimulator instance with the specified listener.
     *
     * @param listener An implementation of the ExternalFeedListener interface that will receive
     *     simulated crypto events. This parameter must not be null, and the listener implementation
     *     should be thread-safe as it will be invoked from multiple threads.
     * @throws IllegalArgumentException if the listener is null
     */
    public FeedSimulator(ExternalFeedListener listener) {
        this.listener = listener;
        this.scheduler = Executors.newScheduledThreadPool(4);
    }

    /**
     * Initializes and starts the feed simulation by creating 10 crypto producers. Each crypto is
     * immediately scheduled for execution with no initial delay (waitTime = 0).
     *
     * <p>This method sets up the simulation environment and triggers the generation of cyrpo data
     * events.
     */
    public void start() {
        for (int i = 0; i < SYMBOL_NAMES.length; i++) {
            scheduleGenerator(new TicketEventProducer(i), (long) 0);
        }
    }

    /**
     * Schedules a recurring task to generate new crypto values.
     *
     * <p>This method schedules a task that computes new crypto values, notifies the listener about
     * these changes, and then reschedules itself with the next computed wait time. The method
     * creates a continuous cycle of crypto value updates with varying intervals.
     *
     * <p>Thread safety is ensured by synchronizing operations on the {@code TicketEventProducer}
     * when manipulating its state.
     *
     * @param tickGenerator the tick producer that will compute new values
     * @param waitTime the time to wait before executing the task, in milliseconds
     */
    private void scheduleGenerator(TicketEventProducer tickGenerator, long waitTime) {
        scheduler.schedule(
                () -> {
                    long nextWaitTime;
                    TickEvent eventData;
                    synchronized (tickGenerator) {
                        tickGenerator.computeNewValues();
                        eventData = tickGenerator.getCurrentValues();
                        nextWaitTime = tickGenerator.computeNextWaitTime();
                    }
                    listener.onEvent(eventData);
                    scheduleGenerator(tickGenerator, nextWaitTime);
                },
                waitTime,
                TimeUnit.MILLISECONDS);
    }

    /**
     * A crypto data simulator that generates realistic crypto price movements.
     *
     * <p>This class simulates a single crypto's price movements and related market data for use in
     * feed simulation scenarios. It maintains state for a crypto including bid/ask values,
     * generating realistic non-divergent random movements centered around reference prices.
     *
     * <p>The simulator uses Gaussian distributions for determining update intervals and provides
     * methods to:
     *
     * <ul>
     *   <li>Compute the next time when an update should occur
     *   <li>Generate new price values for the crypto
     *   <li>Retrieve all current values
     * </ul>
     *
     * <p>Price values are maintained internally as integers (multiplied by 100) for simplicity of
     * calculations and converted to decimal format when retrieved. The price movements follow a
     * random but controlled path that tends to revert to the reference price over time.
     */
    private static class TicketEventProducer {

        /** The reference price of the crypto, used as a baseline for price changes */
        private final int ref;

        /** The mean price of the crypto, used for generating random price changes */
        private final double mean;

        /** The standard deviation of the crypto price, used for generating random price changes */
        private final double stdDev;

        /** The name of the symbol */
        private final String symbol;

        /** The last price of the crypto, used to track the most recent price. */
        private int last;

        /** The other price of the crypto, used for bid/ask calculations. */
        private int other;

        /**
         * Random number generator used to produce simulated data values for the feed. This instance
         * is used across the simulator to ensure consistent random value generation when creating
         * mock data points.
         */
        private final Random random = new Random();

        /**
         * Constructs a TicketEventProducer instance for the stock at the specified index.
         *
         * @param index the index of the crypto in the predefined data arrays
         * @throws IllegalArgumentException if the index is out of bounds for the predefined arrays
         */
        TicketEventProducer(int index) {
            if (index < 0 || index >= SYMBOL_NAMES.length) {
                throw new IllegalArgumentException(
                        "Index out of bounds for predefined arrays: " + index);
            }
            this.symbol = SYMBOL_NAMES[index];

            // All prices are converted in integer form to simplify the management; they will be
            // converted back before being sent  in the update events
            this.ref = (int) Math.round(REF_PRICES[index] * 100);

            this.mean = UPDATE_TIME_MEANS[index];
            this.stdDev = UPDATE_TIME_STD_DEVS[index];
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

        /**
         * Computes new values for the simulated feed data using a controlled random walk algorithm.
         *
         * <p>This method simulates market-like data behavior by: 1) Calculating how far the current
         * value is from a reference point 2) Determining a probability-based direction (toward or
         * away from reference) 3) Applying a random difference to the current value 4) Computing a
         * second value that's slightly different from the main value 5) Updating min/max boundaries
         * when necessary
         *
         * <p>The algorithm includes mean-reversion tendencies, where values far from the reference
         * have a higher probability of moving back toward it.
         */
        void computeNewValues() {
            double limit = ref / 4.0;
            int jump = ref / 100;
            double relDist = (this.last - ref) / limit;
            int direction = 1;
            if (relDist < 0) {
                direction = -1;
                relDist = -relDist;
            }
            if (relDist > 1) {
                relDist = 1;
            }
            double weight = (relDist * relDist * relDist);
            double prob = (1 - weight) / 2;
            boolean goFarther = random.nextDouble() < prob;
            if (!goFarther) {
                direction *= -1;
            }
            int difference = uniform(0, jump) * direction;
            int gap = ref / 250;
            int delta;
            if (gap > 0) {
                do {
                    delta = uniform(-gap, gap);
                } while (delta == 0);
            } else {
                delta = 1;
            }
            this.last += difference;
            this.other = last + delta;
        }

        private int uniform(int min, int max) {
            int base = random.nextInt(max + 1 - min);
            return base + min;
        }

        TickEvent getCurrentValues() {
            BidAsk.Builder bidAsk = BidAsk.newBuilder();
            if (this.other > this.last) {
                bidAsk.setAsk(this.other).setBid(last);
            } else {
                bidAsk.setAsk(this.last).setBid(other);
            }

            return TickEvent.newBuilder()
                    .setTick(
                            Tick.newBuilder()
                                    .setBidAsk(bidAsk.build())
                                    .setSymbol(symbol)
                                    .setTickTime(
                                            Timestamp.newBuilder()
                                                    .setSeconds(
                                                            LocalDateTime.now()
                                                                    .toEpochSecond(ZoneOffset.UTC))
                                                    .build())
                                    .build())
                    .build();
        }
    }
}
