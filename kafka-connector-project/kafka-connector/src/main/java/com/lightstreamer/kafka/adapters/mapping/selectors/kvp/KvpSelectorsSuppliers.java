
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

package com.lightstreamer.kafka.adapters.mapping.selectors.kvp;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.common.utils.Split;
import com.lightstreamer.kafka.common.utils.Split.Splitter;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class KvpSelectorsSuppliers implements KeyValueSelectorSuppliersMaker<String> {

    interface KvpNode extends Node<KvpNode> {

        @Override
        default boolean has(String propertyName) {
            return false;
        }

        @Override
        default KvpNode get(String propertyName) {
            return null;
        }

        @Override
        default KvpNode get(int index) {
            return null;
        }

        @Override
        default boolean isArray() {
            return false;
        }

        @Override
        default int size() {
            return 0;
        }

        @Override
        default boolean isScalar() {
            return true;
        }

        default boolean isNull() {
            return false;
        }
    }

    static class KvpValue implements KvpNode {

        private String value;

        KvpValue(String value) {
            this.value = value;
        }

        @Override
        public String asText() {
            return toString();
        }

        @Override
        public String toString() {
            return value;
        }
    }

    static class KvpMap implements KvpNode {

        static final KvpMap NULL_MAP = new KvpMap(Collections.emptyMap());

        final Map<String, KvpValue> values;

        KvpMap(Map<String, KvpValue> values) {
            this.values = values;
        }

        @Override
        public boolean has(String propertyName) {
            return values.containsKey(propertyName);
        }

        @Override
        public KvpNode get(String propertyName) {
            return values.get(propertyName);
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public String asText() {
            return this != NULL_MAP ? toString() : "";
        }

        @Override
        public String toString() {
            return values.toString();
        }

        @Override
        public boolean isNull() {
            return this == NULL_MAP;
        }

        /**
         * Parses a string into a {@code KvpMap} using the specified delimiters for key-value pairs
         * and key-value separation.
         *
         * @param text the input string to be parsed into a {@code KvpMap}. The string should
         *     contain key-value pairs separated by a semicolon ({@code ;}) and keys and values
         *     separated by an equals sign ({@code =}). If null or blank, {@link KvpMap#NULL_MAP} is
         *     returned.
         * @return a {@code KvpMap} containing the parsed key-value pairs
         */
        static KvpMap fromString(String text) {
            return fromString(text, Split.on(';'), Split.on('='));
        }

        /**
         * Parses a string into a {@code KvpMap} using the provided {@code Splitter} instances for
         * splitting pairs and key-value components.
         *
         * @param text the input string to be parsed. If null or blank, {@link KvpMap#NULL_MAP} is
         *     returned.
         * @param pairs a {@code Splitter} used to split the input string into pairs
         * @param keyValue a {@code Splitter} used to split each pair into key-value components
         * @return a {@code KvpMap} containing the parsed key-value pairs. If the input string is
         *     null or blank, {@link KvpMap#NULL_MAP} is returned.
         */
        static KvpMap fromString(String text, Splitter pairs, Splitter keyValue) {
            if (text == null || text.isBlank()) {
                return KvpMap.NULL_MAP;
            }

            List<String> tokens = pairs.splitToList(text);
            Map<String, KvpValue> values = new LinkedHashMap<>();
            for (int i = 0; i < tokens.size(); i++) {
                String pairToBeSplitted = tokens.get(i);
                keyValue.splitToPair(pairToBeSplitted, true)
                        .ifPresent(pair -> values.put(pair.key(), new KvpValue(pair.value())));
            }

            return new KvpMap(values);
        }
    }

    private static class KvpNodeKeySelectorSupplier implements KeySelectorSupplier<String> {

        private final Deserializer<String> deserializer;
        private final Function<KafkaRecord<String, ?>, KvpMap> kvpMapFactory;

        KvpNodeKeySelectorSupplier(Splitter pair, Splitter keyValue) {
            this.deserializer = Serdes.String().deserializer();
            this.kvpMapFactory = record -> KvpMap.fromString(record.key(), pair, keyValue);
        }

        @Override
        public KeySelector<String> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new KvpNodeKeySelector(name, expression, kvpMapFactory);
        }

        @Override
        public Deserializer<String> deserializer() {
            return deserializer;
        }
    }

    private static final class KvpNodeKeySelector extends StructuredBaseSelector<KvpNode>
            implements KeySelector<String> {

        private final Function<KafkaRecord<String, ?>, KvpMap> kvpMapFactory;

        KvpNodeKeySelector(
                String name,
                ExtractionExpression expression,
                Function<KafkaRecord<String, ?>, KvpMap> kvpMapFactory)
                throws ExtractionException {
            super(name, expression, Constant.KEY);
            this.kvpMapFactory = kvpMapFactory;
        }

        @Override
        public Data extractKey(KafkaRecord<String, ?> record, boolean checkScalar) {
            KvpMap node = kvpMapFactory.apply(record);
            return super.eval(node, checkScalar);
        }
    }

    private static class KvpNodeValueSelectorSupplier implements ValueSelectorSupplier<String> {

        private final Deserializer<String> deserializer;
        private final Function<KafkaRecord<?, String>, KvpMap> kvpMapFactory;

        KvpNodeValueSelectorSupplier(Splitter pair, Splitter keyValue) {
            this.deserializer = Serdes.String().deserializer();
            this.kvpMapFactory = record -> KvpMap.fromString(record.value(), pair, keyValue);
        }

        @Override
        public ValueSelector<String> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new KvpNodeValueSelector(name, expression, kvpMapFactory);
        }

        @Override
        public Deserializer<String> deserializer() {
            return deserializer;
        }
    }

    private static final class KvpNodeValueSelector extends StructuredBaseSelector<KvpNode>
            implements ValueSelector<String> {

        private final Function<KafkaRecord<?, String>, KvpMap> kvpMapFactory;

        KvpNodeValueSelector(
                String name,
                ExtractionExpression expression,
                Function<KafkaRecord<?, String>, KvpMap> kvpMapFactory)
                throws ExtractionException {
            super(name, expression, Constant.VALUE);
            this.kvpMapFactory = kvpMapFactory;
        }

        @Override
        public Data extractValue(KafkaRecord<?, String> record, boolean checkScalar) {
            KvpMap node = kvpMapFactory.apply(record);
            return super.eval(node, checkScalar);
        }
    }

    private final ConnectorConfig config;

    public KvpSelectorsSuppliers(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public KeySelectorSupplier<String> makeKeySelectorSupplier() {
        Splitter pair = Split.on(config.getKeyKvpPairsSeparator());
        Splitter keyValue = Split.on(config.getKeyKvpKeyValueSeparator());
        return new KvpNodeKeySelectorSupplier(pair, keyValue);
    }

    @Override
    public ValueSelectorSupplier<String> makeValueSelectorSupplier() {
        Splitter pair = Split.on(config.getValueKvpPairsSeparator());
        Splitter keyValue = Split.on(config.getValueKvpKeyValueSeparator());
        return new KvpNodeValueSelectorSupplier(pair, keyValue);
    }
}
