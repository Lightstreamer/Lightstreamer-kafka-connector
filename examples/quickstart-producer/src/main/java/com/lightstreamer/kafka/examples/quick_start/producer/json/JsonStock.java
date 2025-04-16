
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

package com.lightstreamer.kafka.examples.quick_start.producer.json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * An interface representing stock data in JSON format.
 *
 * <p>This interface provides functionality to convert event data in map format into a structured
 * {@link Stock} object.
 */
public interface JsonStock {

    /**
     * Converts a map of stock event data into a {@link Stock} object.
     *
     * @param stockEvent a map containing stock data with the following keys:
     *     <ul>
     *       <li>{@code name} - The stock name/symbol
     *       <li>{@code time} - The time of the stock update
     *       <li>{@code timestamp} - The timestamp of the update
     *       <li>{@code last_price} - The last traded price
     *       <li>{@code ask} - The asking price
     *       <li>{@code ask_quantity} - The quantity being asked*
     *       <li>{@code bid} - The bidding price
     *       <li>{@code bid_quantity} - The quantity being bid
     *       <li>{@code pct_change} - The percentage change
     *       <li>{@code min} - The minimum price
     *       <li>{@code max} - The maximum price
     *       <li>{@code ref_price} - The reference price
     *       <li>{@code open_price} - The opening price
     *       <li>{@code item_status} - The status of the stock item
     *     </ul>
     *
     * @return a fully populated {@link JsonStock} object
     * @throws NullPointerException if required fields are missing in the event map
     */
    public static Stock fromEvent(Map<String, String> stockEvent) {
        return new Stock(
                stockEvent.get("name"),
                stockEvent.get("time"),
                stockEvent.get("timestamp"),
                stockEvent.get("last_price"),
                stockEvent.get("ask"),
                stockEvent.get("bid"),
                stockEvent.get("ask_quantity"),
                stockEvent.get("bid_quantity"),
                stockEvent.get("pct_change"),
                stockEvent.get("min"),
                stockEvent.get("max"),
                stockEvent.get("ref_price"),
                stockEvent.get("open_price"),
                stockEvent.get("item_status"));
    }

    String name();

    String time();

    String timestamp();

    String last_price();

    String ask();

    String bid();

    String ask_quantity();

    String bid_quantity();

    String pct_change();

    String min();

    String max();

    String ref_price();

    String open_price();

    String item_status();
}

/**
 * A record representing stock market data that implements the {@link JsonStock} interface.
 *
 * @param name the stock symbol/name
 * @param time the time of the data update in text format
 * @param timestamp the timestamp of the stock data in milliseconds
 * @param last_price the most recent trading price of the stock
 * @param ask the current ask price (sell offer)
 * @param bid the current bid price (buy offer)
 * @param ask_quantity the quantity being offered*
 * @param bid_quantity the quantity being bid for
 * @param pct_change the percentage change in price (usually compared to previous close)
 * @param min the minimum price reached during the current trading session
 * @param max the maximum price reached during the current trading session
 * @param ref_price the reference price used for comparison
 * @param open_price the opening price of the stock item
 * @param item_status the current trading status of the stock
 */
record Stock(
        @JsonProperty String name,
        @JsonProperty String time,
        @JsonProperty String timestamp,
        @JsonProperty String last_price,
        @JsonProperty String ask,
        @JsonProperty String bid,
        @JsonProperty String ask_quantity,
        @JsonProperty String bid_quantity,
        @JsonProperty String pct_change,
        @JsonProperty String min,
        @JsonProperty String max,
        @JsonProperty String ref_price,
        @JsonProperty String open_price,
        @JsonProperty String item_status)
        implements JsonStock {}
