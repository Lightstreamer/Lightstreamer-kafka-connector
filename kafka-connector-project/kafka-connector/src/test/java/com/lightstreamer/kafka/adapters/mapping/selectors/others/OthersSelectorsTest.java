
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
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_KEY_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.ConnectorConfig.RECORD_VALUE_EVALUATOR_TYPE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BOOLEAN;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BYTES;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BYTE_ARRAY;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.BYTE_BUFFER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.DOUBLE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.FLOAT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.INTEGER;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.LONG;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.SHORT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.STRING;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType.UUID;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class OthersSelectorsTest {

    static Stream<Arguments> evaluatorTypes() {
        return Stream.of(
                arguments(STRING, Serdes.String().deserializer().getClass()),
                arguments(INTEGER, Serdes.Integer().deserializer().getClass()),
                arguments(BOOLEAN, Serdes.Boolean().deserializer().getClass()),
                arguments(BYTE_ARRAY, Serdes.ByteArray().deserializer().getClass()),
                arguments(BYTE_BUFFER, Serdes.ByteBuffer().deserializer().getClass()),
                arguments(BYTES, Serdes.Bytes().deserializer().getClass()),
                arguments(DOUBLE, Serdes.Double().deserializer().getClass()),
                arguments(FLOAT, Serdes.Float().deserializer().getClass()),
                arguments(LONG, Serdes.Long().deserializer().getClass()),
                arguments(SHORT, Serdes.Short().deserializer().getClass()),
                arguments(UUID, Serdes.UUID().deserializer().getClass()));
    }

    @Test
    public void shouldConfigWithDefaults() {
        ConnectorConfig config = ConnectorConfigProvider.minimal();
        OthersSelectorSuppliers s = new OthersSelectorSuppliers(config);
        assertThat(s.keyEvaluatorType().is(STRING)).isTrue();
        assertThat(s.valueEvaluatorType().is(STRING)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("evaluatorTypes")
    public void shouldConfigKeyEvaluator(EvaluatorType type, Class<?> expectedDeserializer) {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, type.toString()));
        OthersSelectorSuppliers s = new OthersSelectorSuppliers(config);
        assertThat(s.keyEvaluatorType().is(type)).isTrue();

        KeySelectorSupplier<Object> keySelectorSupplier = s.makeKeySelectorSupplier();
        assertThat(keySelectorSupplier.deserializer()).isInstanceOf(expectedDeserializer);

        assertThat(s.valueEvaluatorType().is(STRING)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("evaluatorTypes")
    public void shouldConfigValueEvaluator(EvaluatorType type, Class<?> expectedDeserializer) {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, type.toString()));
        OthersSelectorSuppliers s = new OthersSelectorSuppliers(config);
        assertThat(s.valueEvaluatorType().is(type)).isTrue();

        ValueSelectorSupplier<Object> valueSelectorSupplier = s.makeValueSelectorSupplier();
        assertThat(valueSelectorSupplier.deserializer()).isInstanceOf(expectedDeserializer);

        assertThat(s.keyEvaluatorType().is(STRING)).isTrue();
    }

    static Stream<Arguments> recordArgs() {
        return Stream.of(
                arguments(STRING, Serdes.String(), "alex"),
                arguments(INTEGER, Serdes.Integer(), 123482),
                arguments(LONG, Serdes.Long(), Long.MAX_VALUE),
                arguments(BOOLEAN, Serdes.Boolean(), false),
                arguments(BOOLEAN, Serdes.Boolean(), true),
                arguments(SHORT, Serdes.Short(), Short.MAX_VALUE),
                arguments(FLOAT, Serdes.Float(), 1.23f),
                arguments(DOUBLE, Serdes.Double(), -121.23d),
                arguments(UUID, Serdes.UUID(), new UUID(4, 5)),
                arguments(BYTES, Serdes.Bytes(), new Bytes(new byte[] {-1, 4, 23, 56, 87})),
                arguments(
                        EvaluatorType.BYTE_ARRAY,
                        Serdes.ByteArray(),
                        new byte[] {-1, 4, 23, 56, 87}),
                arguments(
                        EvaluatorType.BYTE_BUFFER,
                        Serdes.ByteBuffer(),
                        ByteBuffer.wrap(new byte[] {54, -12, 3})));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @ParameterizedTest()
    @MethodSource("recordArgs")
    public void shouldDeserializeAndExtractValue(EvaluatorType type, Serde serde, Object data)
            throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, type.toString()));

        byte[] bytes = serde.serializer().serialize("topic", data);
        OthersSelectorSuppliers othersSelectorSuppliers = new OthersSelectorSuppliers(config);
        ValueSelectorSupplier<?> valueSupplier =
                othersSelectorSuppliers.makeValueSelectorSupplier();
        Object deserializedData = valueSupplier.deserializer().deserialize("topic", bytes);

        KafkaRecord kafkaRecord = Records.fromValue(deserializedData);

        boolean[] checkScalars = {true, false};
        for (boolean checkScalar : checkScalars) {
            String text =
                    valueSupplier
                            .newSelector(Expressions.Expression("VALUE"))
                            .extractValue(kafkaRecord, checkScalar)
                            .text();
            assertThat(text).isEqualTo(String.valueOf(data));
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldExtractNullValue() throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, INTEGER.toString()));
        ValueSelectorSupplier<?> valueSupplier =
                new OthersSelectorSuppliers(config).makeValueSelectorSupplier();
        KafkaRecord kafkaRecord = Records.fromValue((Object) null);

        boolean[] checkScalars = {true, false};
        for (boolean checkScalar : checkScalars) {
            String text =
                    valueSupplier
                            .newSelector(Expressions.Expression("VALUE"))
                            .extractValue(kafkaRecord, checkScalar)
                            .text();
            assertThat(text).isNull();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @ParameterizedTest()
    @MethodSource("recordArgs")
    public void shouldDeserializeAndExtractKey(EvaluatorType type, Serde serde, Object data)
            throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, type.toString()));
        KeySelectorSupplier<?> keySupplier =
                new OthersSelectorSuppliers(config).makeKeySelectorSupplier();
        byte[] bytes = serde.serializer().serialize("topic", data);
        Object deserializedData = keySupplier.deserializer().deserialize("topic", bytes);

        KafkaRecord kafkaRecord = Records.fromKey(deserializedData);

        boolean[] checkScalars = {true, false};
        for (boolean checkScalar : checkScalars) {
            String text =
                    keySupplier
                            .newSelector(Expressions.Expression("KEY"))
                            .extractKey(kafkaRecord, checkScalar)
                            .text();
            assertThat(text).isEqualTo(String.valueOf(data));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldExtractNullKey() throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_KEY_EVALUATOR_TYPE, INTEGER.toString()));
        KeySelectorSupplier<?> keySupplier =
                new OthersSelectorSuppliers(config).makeKeySelectorSupplier();
        KafkaRecord kafkaRecord = Records.fromKey((Object) null);

        boolean[] checkScalars = {true, false};
        for (boolean checkScalar : checkScalars) {
            String text =
                    keySupplier
                            .newSelector(Expressions.Expression("KEY"))
                            .extractKey(kafkaRecord, checkScalar)
                            .text();
            assertThat(text).isNull();
        }
    }

    @ParameterizedTest()
    @MethodSource("recordArgs")
    public void shouldNotCreateSelector(EvaluatorType type) throws ExtractionException {
        ConnectorConfig config =
                ConnectorConfigProvider.minimalWith(
                        Map.of(RECORD_VALUE_EVALUATOR_TYPE, INTEGER.toString()));
        ValueSelectorSupplier<?> valueSupplier =
                new OthersSelectorSuppliers(config).makeValueSelectorSupplier();
        ExtractionExpression expression = Expressions.Expression("VALUE.a");
        ExtractionException ee =
                assertThrows(
                        ExtractionException.class,
                        () -> {
                            valueSupplier.newSelector(expression);
                        });
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Found the invalid expression [VALUE.a] for scalar values");
    }
}
