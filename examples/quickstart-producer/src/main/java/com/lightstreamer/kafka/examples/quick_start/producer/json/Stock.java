
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

/**
 * A record representing stock market data.
 *
 * @param name the stock symbol/name
 * @param time the time of the data update in text format
 * @param timestamp the timestamp of the stock data in milliseconds
 * @param last_price the most recent trading price of the stock
 * @param ask the current ask price (sell offer)
 * @param bid the current bid price (buy offer)
 * @param ask_quantity the quantity being offered
 * @param bid_quantity the quantity being bid for
 * @param pct_change the percentage change in price (usually compared to previous close)
 * @param min the minimum price reached during the current trading session
 * @param max the maximum price reached during the current trading session
 * @param ref_price the reference price used for comparison
 * @param open_price the opening price of the stock item
 * @param item_status the current trading status of the stock
 */
public record Stock(
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
        @JsonProperty String item_status) {}
