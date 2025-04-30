
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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A simulator for generating synthetic financial market data feeds.
 *
 * <p>This class simulates a real-time financial data feed by generating realistic price movements
 * and other market metrics for a collection of fictional stocks. It's designed to produce a
 * continuous stream of market events with variable timing and non-divergent price walks, mimicking
 * the behavior of an external financial data broadcast feed.
 *
 * <p>The simulator:
 *
 * <ul>
 *   <li>Generates data for 10 different simulated stocks
 *   <li>Produces realistic price movements that tend to revert to reference prices
 *   <li>Uses Gaussian distributions to create natural-seeming update intervals
 *   <li>Delivers events through a listener pattern to registered clients
 * </ul>
 *
 * <p>Each simulated stock includes:
 *
 * <ul>
 *   <li>Stock name and price information (last price, bid, ask)
 *   <li>Trading volumes and quantities
 *   <li>Percentage changes and reference values
 *   <li>Timestamps and status information
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 *   ExternalFeedListener myListener = (index, event) -> {
 *       // Process the stock update event
 *       System.out.println("Stock " + index + " update: " + event);
 *   };
 *
 *   FeedSimulator simulator = new FeedSimulator(myListener);
 *   simulator.start();
 * </pre>
 *
 * <p>The simulation runs on a scheduled thread pool and will continue producing events until
 * explicitly stopped or the application terminates.
 */
public class FeedSimulator {

    /**
     * A listener interface for receiving events from an external feed simulator. Implementations of
     * this interface can be registered with a feed simulator to be notified when new stock events
     * are generated.
     */
    public interface ExternalFeedListener {

        /**
         * Callback method invoked when a stock event occurs.
         *
         * @param stockIndex the index of the stock that generated the event
         * @param event a map containing the event data as key-value pairs
         */
        void onEvent(int stockIndex, Map<String, String> event);
    }

    /**
     * Used to automatically generate the updates for the 30 stocks: mean and standard deviation of
     * the times between consecutive updates for the same stock.
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
     * Array of reference stock prices used for simulating market data. These values serve as base
     * prices for generating realistic price movements in the simulated feed. Each value represents
     * a different stock's reference price in currency units (likely USD).
     */
    private static final double[] REF_PRICES = {
        3.04, 16.09, 7.19, 3.63, 7.61, 2.30, 15.39, 5.31, 4.86, 7.61, 10.41, 3.94, 6.79, 26.87,
        2.27, 13.04, 6.09, 17.19, 13.63, 17.61, 11.30, 5.39, 15.31, 14.86, 17.61, 5.41, 13.94,
        16.79, 6.87, 11.27,
    };

    /**
     * Array of fixed opening prices used for market data simulation. Each value represents the
     * initial price point for a different financial instrument in the simulation. These values are
     * used as the base prices for generating market data fluctuations in the feed simulator.
     */
    private static final double[] OPEN_PRICES = {
        3.10, 16.20, 7.25, 3.62, 7.65, 2.30, 15.85, 5.31, 4.97, 7.70, 10.50, 3.95, 6.84, 27.05,
        2.29, 13.20, 6.20, 17.25, 13.62, 17.65, 11.30, 5.55, 15.31, 14.97, 17.70, 5.42, 13.95,
        16.84, 7.05, 11.29,
    };

    /**
     * An array of minimum prices used for simulating financial instrument price fluctuations. Each
     * value represents the minimum price threshold for a specific stock or financial instrument in
     * the simulation. These values are used as the lower bound when generating random price
     * movements. The array contains 30 predefined minimum price points.
     */
    private static final double[] MIN_PRICES = {
        3.09, 15.78, 7.15, 3.62, 7.53, 2.28, 15.60, 5.23, 4.89, 7.70, 10.36, 3.90, 6.81, 26.74,
        2.29, 13.09, 5.78, 17.15, 13.62, 17.53, 11.28, 5.60, 15.23, 14.89, 17.70, 5.36, 13.90,
        16.81, 6.74, 11.29,
    };

    /**
     * Array containing the maximum price values for simulated financial instruments. These values
     * represent upper bounds for price fluctuations in the feed simulation. Each value corresponds
     * to a different instrument in the simulated market data feed. The simulator will generate
     * random price movements that will not exceed these maximum values.
     */
    private static final double[] MAX_PRICES = {
        3.19, 16.20, 7.26, 3.71, 7.65, 2.30, 15.89, 5.31, 4.97, 7.86, 10.50, 3.95, 6.87, 27.05,
        2.31, 13.19, 6.20, 17.26, 13.71, 17.65, 11.30, 5.89, 15.31, 14.97, 17.86, 5.50, 13.95,
        16.87, 7.05, 11.31,
    };

    /**
     * Array of fictional company names used to simulate stock data in the feed simulator. These
     * names represent mock stocks for testing and demonstration purposes within the Kafka connector
     * example. Each element represents a company name that will be used when generating simulated
     * market data events.
     */
    private static final String[] STOCK_NAMES = {
        "Anduct", "Ations Europe", "Bagies Consulting", "BAY Corporation",
        "CON Consulting", "Corcor PLC", "CVS Asia", "Datio PLC",
        "Dentems", "ELE Manufacturing", "Exacktum Systems", "KLA Systems Inc",
        "Lted Europe", "Magasconall Capital", "MED", "Mice Investments",
        "Micropline PLC", "Nologicroup Devices", "Phing Technology", "Pres Partners",
        "Quips Devices", "Ress Devices", "Sacle Research", "Seaging Devices",
        "Sems Systems, Inc", "Softwora Consulting", "Systeria Develop", "Thewlec Asia",
        "Virtutis", "Yahl"
    };

    /** Used to keep the contexts of the 30 stocks. */
    private final List<StockProducer> stockGenerators = new ArrayList<>();

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
     *     simulated stock events. This parameter must not be null, and the listener implementation
     *     should be thread-safe as it will be invoked from multiple threads.
     * @throws IllegalArgumentException if the listener is null
     */
    public FeedSimulator(ExternalFeedListener listener) {
        this.listener = listener;
        this.scheduler = Executors.newScheduledThreadPool(4);
    }

    /**
     * Initializes and starts the feed simulation by creating 10 stock producers. Each stock
     * producer is added to the stockGenerators collection and immediately scheduled for execution
     * with no initial delay (waitTime = 0).
     *
     * <p>This method sets up the simulation environment and triggers the generation of stock market
     * data events.
     */
    public void start() {
        for (int i = 0; i < 10; i++) {
            StockProducer stock = new StockProducer(i);
            stockGenerators.add(stock);
            long waitTime = 0;
            scheduleGenerator(stock, waitTime);
        }
    }

    /**
     * Schedules a recurring task to generate new stock values.
     *
     * <p>This method schedules a task that computes new stock values, notifies the listener about
     * these changes, and then reschedules itself with the next computed wait time. The method
     * creates a continuous cycle of stock value updates with varying intervals.
     *
     * <p>Thread safety is ensured by synchronizing operations on the {@code stockProducer} when
     * manipulating its state.
     *
     * @param stockProducer the stock producer that will compute new values
     * @param waitTime the time to wait before executing the task, in milliseconds
     */
    private void scheduleGenerator(StockProducer stockProducer, long waitTime) {
        scheduler.schedule(
                () -> {
                    long nextWaitTime;
                    Map<String, String> eventData;
                    synchronized (stockProducer) {
                        stockProducer.computeNewValues();
                        eventData = stockProducer.getCurrentValues();
                        nextWaitTime = stockProducer.computeNextWaitTime();
                    }
                    listener.onEvent(stockProducer.index, eventData);
                    scheduleGenerator(stockProducer, nextWaitTime);
                },
                waitTime,
                TimeUnit.MILLISECONDS);
    }

    /**
     * A stock data simulator that generates realistic stock price movements.
     *
     * <p>This class simulates a single stock's price movements and related market data for use in
     * feed simulation scenarios. It maintains state for a stock including prices, bid/ask values,
     * and min/max values, generating realistic non-divergent random movements centered around
     * reference prices.
     *
     * <p>The simulator uses Gaussian distributions for determining update intervals and provides
     * methods to:
     *
     * <ul>
     *   <li>Compute the next time when an update should occur
     *   <li>Generate new price values for the stock
     *   <li>Retrieve all current values in a field-to-value map format
     * </ul>
     *
     * <p>Price values are maintained internally as integers (multiplied by 100) for simplicity of
     * calculations and converted to decimal format when retrieved. The price movements follow a
     * random but controlled path that tends to revert to the reference price over time.
     */
    private static class StockProducer {

        /** The unique identifier for this feed simulator instance */
        private final int index;

        /** The initial price of the stock when the feed simulator starts */
        private final int open;

        /** The reference price of the stock, used as a baseline for price changes */
        private final int ref;

        /** The mean price of the stock, used for generating random price changes */
        private final double mean;

        /** The standard deviation of the stock price, used for generating random price changes */
        private final double stdDev;

        /** The name of the stock, used for identification in the feed simulator */
        private final String name;

        /** The minimum price of the stock */
        private int min;

        /** The maximum price of the stock */
        private int max;

        /** The last price of the stock, used to track the most recent price. */
        private int last;

        /** The other price of the stock, used for bid/ask calculations. */
        private int other;

        /**
         * Random number generator used to produce simulated data values for the feed. This instance
         * is used across the simulator to ensure consistent random value generation when creating
         * mock data points.
         */
        private final Random random = new Random();

        /**
         * Constructs a StockProducer instance for the stock at the specified index.
         *
         * @param index the index of the stock in the predefined data arrays
         * @throws IllegalArgumentException if the index is out of bounds for the predefined arrays
         */
        StockProducer(int index) {
            if (index < 0 || index >= OPEN_PRICES.length) {
                throw new IllegalArgumentException(
                        "Index out of bounds for predefined arrays: " + index);
            }
            this.index = index;
            this.name = STOCK_NAMES[index];

            // All prices are converted in integer form to simplify the management; they will be
            // converted back before being sent  in the update events
            this.open = (int) Math.round(OPEN_PRICES[index] * 100);
            this.ref = (int) Math.round(REF_PRICES[index] * 100);
            this.min = (int) Math.ceil(MIN_PRICES[index] * 100);
            this.max = (int) Math.floor(MAX_PRICES[index] * 100);

            this.last = open;
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
            if (last < this.min) {
                this.min = last;
            }
            if (last > this.max) {
                this.max = this.last;
            }
        }

        private int uniform(int min, int max) {
            int base = random.nextInt(max + 1 - min);
            return base + min;
        }

        Map<String, String> getCurrentValues() {
            HashMap<String, String> event = new HashMap<String, String>();
            event.put("name", this.name);

            LocalDateTime now = LocalDateTime.now();
            event.put("time", now.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            event.put("timestamp", String.valueOf(now.toInstant(ZoneOffset.UTC).toEpochMilli()));
            addDecField("last_price", this.last, event);
            if (this.other > this.last) {
                addDecField("ask", this.other, event);
                addDecField("bid", this.last, event);
            } else {
                addDecField("ask", this.last, event);
                addDecField("bid", this.other, event);
            }
            event.put("bid_quantity", String.valueOf(uniform(1, 200) * 500));
            event.put("ask_quantity", String.valueOf(uniform(1, 200) * 500));

            double var = (this.last - this.ref) / (double) this.ref * 100;
            addDecField("pct_change", (int) (var * 100), event);
            addDecField("min", this.min, event);
            addDecField("max", this.max, event);

            addDecField("ref_price", this.ref, event);
            addDecField("open_price", this.open, event);
            // since it's a simulator the item is always active
            event.put("item_status", "active");
            return event;
        }

        /**
         * Adds a decimal field to the target HashMap. This method converts an integer value (that
         * represents a decimal value multiplied by 100) to a properly formatted decimal string with
         * 2 decimal places.
         *
         * @param fld the field name to use as key in the target HashMap
         * @param val100 the integer value representing the decimal value multiplied by 100
         * @param target the HashMap to which the field-value pair will be added
         */
        private void addDecField(String fld, int val100, HashMap<String, String> target) {
            String formattedValue = String.format("%.2f", val100 / 100.0);
            target.put(fld, formattedValue);
        }
    }
}
