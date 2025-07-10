
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

import static com.google.common.truth.Truth.assertThat;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.InvalidProtocolBufferException;
import com.lightstreamer.kafka.examples.quick_start.producer.test_utils.StockEvents;

import org.junit.jupiter.api.Test;

import java.util.Map;

public class CustomProtobufSerializerTest {

    @Test
    public void shouldSerialize() throws InvalidProtocolBufferException {
        // Create test stock event
        Map<String, String> stockEvent = StockEvents.createEvent();

        Stock stock = ProtobufStock.fromEvent(stockEvent);
        assertNotNull(stock);

        // Serialize the stock object to bytes
        try (CustomProtobufSerializer serializer = new CustomProtobufSerializer()) {
            byte[] serializedStock = serializer.serialize("aTopic", stock);

            // Deserialize back to a Stock object
            Stock deserializedStock = Stock.parseFrom(serializedStock);
            assertThat(deserializedStock).isEqualTo(stock);
        }
    }
}
