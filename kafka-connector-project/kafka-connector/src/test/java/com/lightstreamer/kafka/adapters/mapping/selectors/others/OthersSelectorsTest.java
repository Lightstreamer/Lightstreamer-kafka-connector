
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

package com.lightstreamer.kafka.adapters.mapping.selectors.others;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BOOLEAN;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BYTES;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.DOUBLE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.FLOAT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.INTEGER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.SHORT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.test_utils.ConsumerRecords;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.BooleanSerde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.ByteBufferSerde;
import org.apache.kafka.common.serialization.Serdes.BytesSerde;
import org.apache.kafka.common.serialization.Serdes.DoubleSerde;
import org.apache.kafka.common.serialization.Serdes.FloatSerde;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.ShortSerde;
import org.apache.kafka.common.serialization.Serdes.UUIDSerde;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

public class OthersSelectorsTest {

    static ValueSelector<?> valueSelector(EvaluatorType type, String expression) {
        return OthersSelectorSuppliers.valueSelectorSupplier(type).newSelector("name", expression);
    }

    static KeySelector<?> keySelector(EvaluatorType type, String expression) {
        return OthersSelectorSuppliers.keySelectorSupplier(type).newSelector("name", expression);
    }

    static ValueSelectorSupplier<?> valueSelectorSupplier(EvaluatorType type) {
        return OthersSelectorSuppliers.valueSelectorSupplier(type);
    }

    static KeySelectorSupplier<?> keySelectorSupplier(EvaluatorType type) {
        return OthersSelectorSuppliers.keySelectorSupplier(type);
    }

    static Stream<Arguments> recordArgs() {
        return Stream.of(
                arguments(INTEGER, new IntegerSerde(), 123482),
                arguments(LONG, new LongSerde(), Long.MAX_VALUE),
                arguments(BOOLEAN, new BooleanSerde(), false),
                arguments(BOOLEAN, new BooleanSerde(), true),
                arguments(SHORT, new ShortSerde(), Short.MAX_VALUE),
                arguments(FLOAT, new FloatSerde(), 1.23f),
                arguments(DOUBLE, new DoubleSerde(), -121.23d),
                arguments(UUID, new UUIDSerde(), new UUID(4, 5)),
                arguments(BYTES, new BytesSerde(), new Bytes(new byte[] {-1, 4, 23, 56, 87})),
                arguments(
                        EvaluatorType.BYTE_ARRAY,
                        new ByteArraySerde(),
                        new byte[] {-1, 4, 23, 56, 87}),
                arguments(
                        EvaluatorType.BYTE_BUFFER,
                        new ByteBufferSerde(),
                        ByteBuffer.wrap(new byte[] {54, -12, 3})));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @ParameterizedTest()
    @MethodSource("recordArgs")
    public void shouldDeserializeAndExtractValue(EvaluatorType type, Serde serde, Object data) {
        byte[] bytes = serde.serializer().serialize("topic", data);
        ValueSelectorSupplier<?> valueSupplier = valueSelectorSupplier(type);
        Object deserializedData = valueSupplier.deseralizer().deserialize("topic", bytes);

        KafkaRecord kafkaRecord = ConsumerRecords.fromValue(deserializedData);
        String text = valueSupplier.newSelector("name", "VALUE").extract(kafkaRecord).text();
        assertThat(text).isEqualTo(String.valueOf(data));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldExtractNullValue() {
        ValueSelectorSupplier<?> valueSupplier = valueSelectorSupplier(EvaluatorType.INTEGER);
        KafkaRecord kafkaRecord = ConsumerRecords.fromValue((Object) null);
        String text = valueSupplier.newSelector("name", "VALUE").extract(kafkaRecord).text();
        assertThat(text).isNull();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldExtractNullKey() {
        KeySelectorSupplier<?> valueSupplier = keySelectorSupplier(EvaluatorType.INTEGER);
        KafkaRecord kafkaRecord = ConsumerRecords.fromKey((Object) null);
        String text = valueSupplier.newSelector("name", "KEY").extract(kafkaRecord).text();
        assertThat(text).isNull();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @ParameterizedTest()
    @MethodSource("recordArgs")
    public void shouldDeserializeAndExtractKey(EvaluatorType type, Serde serde, Object data) {
        byte[] bytes = serde.serializer().serialize("topic", data);
        KeySelectorSupplier<?> valueSupplier = keySelectorSupplier(type);
        Object deserializedData = valueSupplier.deseralizer().deserialize("topic", bytes);

        KafkaRecord kafkaRecord = ConsumerRecords.fromKey(deserializedData);
        String text = valueSupplier.newSelector("name", "KEY").extract(kafkaRecord).text();
        assertThat(text).isEqualTo(String.valueOf(data));
    }

    @Test
    public void shouldNotCreate() {
        ExpressionException ee1 =
                assertThrows(ExpressionException.class, () -> keySelector(SHORT, "invalidKey"));
        assertThat(ee1.getMessage())
                .isEqualTo("Expected the root token [KEY] while evaluating [name]");

        ExpressionException ee2 =
                assertThrows(ExpressionException.class, () -> keySelector(SHORT, ""));
        assertThat(ee2.getMessage())
                .isEqualTo("Expected the root token [KEY] while evaluating [name]");

        ExpressionException ee3 =
                assertThrows(ExpressionException.class, () -> valueSelector(SHORT, "invalidValue"));
        assertThat(ee3.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [name]");

        ExpressionException ee4 =
                assertThrows(ExpressionException.class, () -> valueSelector(SHORT, ""));
        assertThat(ee4.getMessage())
                .isEqualTo("Expected the root token [VALUE] while evaluating [name]");
    }
}
