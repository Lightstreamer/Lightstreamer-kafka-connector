
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

package com.lightstreamer.kafka.examples.quick_start.producer.protobuf;

import java.util.Map;

/**
 * Provides conversion utilities for creating Protocol Buffer {@link Stock} objects from
 * string-based data.
 *
 * <p>This interface provides functionality to convert event data in map format into a into strongly
 * typed Protocol Buffer {@link Stock} objects.
 */
public interface ProtobufStock {

    /**
     * Converts a map of stock data fields into a Protocol Buffer {@link Stock} object.
     *
     * @param stockEvent a map containing stock data with the following keys:
     *     <ul>
     *       <li>{@code name} - The stock name/symbol
     *       <li>{@code time} - The time of the stock update
     *       <li>{@code timestamp} - The timestamp of the update
     *       <li>{@code last_price} - The last traded price
     *       <li>{@code ask} - The asking price
     *       <li>{@code ask_quantity} - The quantity being asked
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
     * @return a fully populated {@link Stock} Protocol Buffer message
     */
    public static Stock fromEvent(Map<String, String> stockEvent) {
        return Stock.newBuilder()
                .setName(stockEvent.get("name"))
                .setTime(stockEvent.get("time"))
                .setTimestamp(stockEvent.get("timestamp"))
                .setLastPrice(stockEvent.get("last_price"))
                .setAsk(stockEvent.get("ask"))
                .setBid(stockEvent.get("bid"))
                .setBidQuantity(stockEvent.get("bid_quantity"))
                .setAskQuantity(stockEvent.get("ask_quantity"))
                .setPctChange(stockEvent.get("pct_change"))
                .setMin(stockEvent.get("min"))
                .setMax(stockEvent.get("max"))
                .setRefPrice(stockEvent.get("ref_price"))
                .setOpenPrice(stockEvent.get("open_price"))
                .setItemStatus(stockEvent.get("item_status"))
                .build();
    }
}
