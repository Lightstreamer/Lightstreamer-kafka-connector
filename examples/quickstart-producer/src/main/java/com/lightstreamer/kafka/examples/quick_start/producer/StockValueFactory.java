
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

import com.lightstreamer.kafka.examples.quick_start.producer.json.Stock;

import java.util.Map;

/**
 * Factory class for creating stock message values in each supported {@link
 * Producer.SerializationFormat}.
 *
 * <p>Each method accepts a raw stock event map (field name → string value) and returns the
 * strongly-typed value object ready to be placed in a Kafka {@link
 * org.apache.kafka.clients.producer.ProducerRecord}.
 */
public final class StockValueFactory {

    private StockValueFactory() {}

    /**
     * Creates a Protocol Buffer {@link
     * com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock} from a stock event.
     *
     * @param stockEvent a map of stock field names to their string values
     * @return a fully populated Protobuf Stock message
     */
    public static com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock protobuf(
            Map<String, String> stockEvent) {
        return com.lightstreamer.kafka.examples.quick_start.producer.protobuf.Stock.newBuilder()
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

    /**
     * Creates an Avro {@link com.lightstreamer.kafka.examples.quick_start.producer.avro.Stock} from
     * a stock event.
     *
     * @param stockEvent a map of stock field names to their string values
     * @return a fully populated Avro Stock record
     */
    public static com.lightstreamer.kafka.examples.quick_start.producer.avro.Stock avro(
            Map<String, String> stockEvent) {
        return com.lightstreamer.kafka.examples.quick_start.producer.avro.Stock.newBuilder()
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

    /**
     * Creates a JSON {@link Stock} from a stock event.
     *
     * @param stockEvent a map of stock field names to their string values
     * @return a fully populated JSON Stock record
     */
    public static Stock json(Map<String, String> stockEvent) {
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
}
