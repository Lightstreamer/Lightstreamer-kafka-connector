
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

package com.lightstreamer.kafka_connector.samples.producer;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Simulates an external data feed that supplies quote values for all the stocks needed for the
 * demo.
 */
public class FeedSimluator {

    public interface ExternalFeedListener {

        void onEvent(Stock stock, boolean b);
    }

    private static final Timer dispatcher = new Timer();
    private static final Random random = new Random();

    /**
     * Used to automatically generate the updates for the 30 stocks: mean and standard deviation of
     * the times between consecutive updates for the same stock.
     */
    private static final double[] updateTimeMeans = {
        30000, 500, 3000, 90000,
        7000, 10000, 3000, 7000,
        7000, 7000, 500, 3000,
        20000, 20000, 20000, 30000,
        500, 3000, 90000, 7000,
        10000, 3000, 7000, 7000,
        7000, 500, 3000, 20000,
        20000, 20000,
    };

    private static final double[] updateTimeStdDevs = {
        6000, 300, 1000, 1000,
        100, 5000, 1000, 3000,
        1000, 6000, 300, 1000,
        1000, 4000, 1000, 6000,
        300, 1000, 1000, 100,
        5000, 1000, 3000, 1000,
        6000, 300, 1000, 1000,
        4000, 1000,
    };

    /** Used to generate the initial field values for the 30 stocks. */
    private static final double[] refprices = {
        3.04, 16.09, 7.19, 3.63, 7.61, 2.30, 15.39, 5.31, 4.86, 7.61, 10.41, 3.94, 6.79, 26.87,
        2.27, 13.04, 6.09, 17.19, 13.63, 17.61, 11.30, 5.39, 15.31, 14.86, 17.61, 5.41, 13.94,
        16.79, 6.87, 11.27,
    };

    private static final double[] openprices = {
        3.10, 16.20, 7.25, 3.62, 7.65, 2.30, 15.85, 5.31, 4.97, 7.70, 10.50, 3.95, 6.84, 27.05,
        2.29, 13.20, 6.20, 17.25, 13.62, 17.65, 11.30, 5.55, 15.31, 14.97, 17.70, 5.42, 13.95,
        16.84, 7.05, 11.29,
    };
    private static final double[] minprices = {
        3.09, 15.78, 7.15, 3.62, 7.53, 2.28, 15.60, 5.23, 4.89, 7.70, 10.36, 3.90, 6.81, 26.74,
        2.29, 13.09, 5.78, 17.15, 13.62, 17.53, 11.28, 5.60, 15.23, 14.89, 17.70, 5.36, 13.90,
        16.81, 6.74, 11.29,
    };
    private static final double[] maxprices = {
        3.19, 16.20, 7.26, 3.71, 7.65, 2.30, 15.89, 5.31, 4.97, 7.86, 10.50, 3.95, 6.87, 27.05,
        2.31, 13.19, 6.20, 17.26, 13.71, 17.65, 11.30, 5.89, 15.31, 14.97, 17.86, 5.50, 13.95,
        16.87, 7.05, 11.31,
    };
    private static final String[] stockNames = {
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

    /** Used to keep the contexts of the 30 stocks. */
    private final ArrayList<StockProducer> stockGenerators = new ArrayList<>();

    private ExternalFeedListener listener;

    /**
     * Starts generating update events for the stocks. Sumulates attaching and reading from an
     * external broadcast feed.
     */
    public void start() {
        for (int i = 0; i < 30; i++) {
            StockProducer stock = new StockProducer(i);
            stockGenerators.add(stock);
            long waitTime = stock.computeNextWaitTime();
            scheduleGenerator(stock, waitTime);
        }
    }

    /**
     * Sets an internal listener for the update events. Since now, the update events were ignored.
     */
    public void setFeedListener(ExternalFeedListener listener) {
        this.listener = listener;
    }

    /**
     * Generates new values and sends a new update event at the time the producer declared to do it.
     */
    private void scheduleGenerator(final StockProducer stock, long waitTime) {
        dispatcher.schedule(
                new TimerTask() {
                    public void run() {
                        long nextWaitTime;
                        synchronized (stock) {
                            stock.computeNewValues();
                            if (listener != null) {
                                listener.onEvent(stock.getCurrentValues(false), false);
                            }
                            nextWaitTime = stock.computeNextWaitTime();
                        }
                        scheduleGenerator(stock, nextWaitTime);
                    }
                },
                waitTime);
    }

    /** Manages the current state and generates update events for a single stock. */
    private static class StockProducer {
        private final int open, ref;
        private final double mean, stddev;
        private final String name;
        private int min, max, last, other;

        /** Initializes stock data based on the already prepared values. */
        public StockProducer(int itemPos) {
            // All prices are converted in integer form to simplify the
            // management; they will be converted back before being sent
            // in the update events
            open = (int) Math.round(openprices[itemPos] * 100);
            ref = (int) Math.round(refprices[itemPos] * 100);
            min = (int) Math.ceil(minprices[itemPos] * 100);
            max = (int) Math.floor(maxprices[itemPos] * 100);
            name = stockNames[itemPos];
            last = open;
            mean = updateTimeMeans[itemPos];
            stddev = updateTimeStdDevs[itemPos];
        }

        /**
         * Decides, for ease of simulation, the time at which the next update for the stock will
         * happen.
         */
        public long computeNextWaitTime() {
            long millis;
            do {
                millis = (long) gaussian(mean, stddev);
            } while (millis <= 0);
            return millis;
        }

        /** Changes the current data for the stock. */
        public void computeNewValues() {
            // this stuff is to ensure that new prices follow a random
            // but nondivergent path, centered around the reference price
            double limit = ref / 4.0;
            int jump = ref / 100;
            double relDist = (last - ref) / limit;
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
            last += difference;
            other = last + delta;
            if (last < min) {
                min = last;
            }
            if (last > max) {
                max = last;
            }
        }

        /**
         * Picks the stock field values and stores them in a <field->value> HashMap. If fullData is
         * false, then only the fields whose value is just changed are considered (though this check
         * is not strict).
         */
        public Stock getCurrentValues(boolean fullData) {
            Stock stock = new Stock();
            stock.name = name;
            final HashMap<String, String> event = new HashMap<String, String>();

            String format = "HH:mm:ss";
            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date now = new Date();
            event.put("time", formatter.format(now));
            event.put("timestamp", Long.toString(now.getTime()));
            stock.time = formatter.format(now);
            stock.timestamp = Long.toString(now.getTime());
            addDecField("last_price", last, event);
            stock.last_price = addDecField(last);
            if (other > last) {
                addDecField("ask", other, event);
                stock.ask = addDecField(other);
                addDecField("bid", last, event);
                stock.bid = addDecField(last);
            } else {
                addDecField("ask", last, event);
                stock.ask = addDecField(last);
                addDecField("bid", other, event);
                stock.bid = addDecField(other);
            }
            int quantity;
            quantity = uniform(1, 200) * 500;
            event.put("bid_quantity", Integer.toString(quantity));
            stock.bid_quantity = Integer.toString(quantity);

            quantity = uniform(1, 200) * 500;
            event.put("ask_quantity", Integer.toString(quantity));
            stock.ask_quantity = Integer.toString(quantity);

            double var = (last - ref) / (double) ref * 100;
            addDecField("pct_change", (int) (var * 100), event);
            stock.pct_change = addDecField((int) (var * 100));
            if ((last == min) || fullData) {
                addDecField("min", min, event);
                stock.min = addDecField(min);
            }
            if ((last == max) || fullData) {
                addDecField("max", max, event);
                stock.max = addDecField(max);
            }
            if (fullData) {
                event.put("stock_name", name);
                addDecField("ref_price", ref, event);
                stock.ref_price = addDecField(ref);
                addDecField("open_price", open, event);
                stock.open_price = addDecField(open);
                // since it's a simulator the item is always active
                event.put("item_status", "active");
                stock.item_status = "active";
            }
            return stock;
        }

        private void addDecField(String fld, int val100, HashMap<String, String> target) {
            double val = (((double) val100) / 100);
            String buf = Double.toString(val);
            target.put(fld, buf);
        }

        private String addDecField(int val100) {
            return Double.toString((((double) val100) / 100));
        }

        private long gaussian(double mean, double stddev) {
            double base = random.nextGaussian();
            return (long) (base * stddev + mean);
        }

        private int uniform(int min, int max) {
            int base = random.nextInt(max + 1 - min);
            return base + min;
        }
    }

    public static class Stock {
        @JsonProperty public String name;
        @JsonProperty public String time;
        @JsonProperty public String timestamp;
        @JsonProperty public String last_price;
        @JsonProperty public String ask;
        @JsonProperty public String bid;
        @JsonProperty public String bid_quantity;
        @JsonProperty public String ask_quantity;
        @JsonProperty public String pct_change;
        @JsonProperty public String min;
        @JsonProperty public String max;
        @JsonProperty public String ref_price;
        @JsonProperty public String open_price;
        @JsonProperty public String item_status;
    }
}
