
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
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
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
import java.util.function.BiFunction;

public class KvpSelectorsSuppliers implements KeyValueSelectorSuppliersMaker<String> {

    interface KvpNode extends Node<KvpNode> {

        default boolean isArray() {
            return false;
        }

        default int size() {
            return 0;
        }

        default KvpNode get(int index) {
            return null;
        }
    }

    static class KvpValue implements KvpNode {

        private final String name;
        private final String value;

        KvpValue(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String text() {
            return toString();
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public boolean has(String propertyname) {
            return false;
        }

        @Override
        public KvpNode get(String propertyName) {
            return null;
        }

        @Override
        public boolean isScalar() {
            return true;
        }
    }

    static class KvpMap implements KvpNode {

        static final KvpMap NULL_MAP = new KvpMap("NULL_MAP", Collections.emptyMap());

        private final String name;
        private final Map<String, KvpValue> values;

        KvpMap(String name, Map<String, KvpValue> values) {
            this.name = name;
            this.values = values;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isNull() {
            return this == NULL_MAP;
        }

        @Override
        public boolean isScalar() {
            return false;
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
        public String text() {
            return this != NULL_MAP ? toString() : "";
        }

        @Override
        public String toString() {
            return values.toString();
        }

        /**
         * Parses a string into a {@code KvpMap} using the specified delimiters for key-value pairs
         * and key-value separation.
         *
         * @param name the name to be assigned to the created {@code KvpMap}
         * @param text the input string to be parsed into a {@code KvpMap}. The string should
         *     contain key-value pairs separated by a semicolon ({@code ;}) and keys and values
         *     separated by an equals sign ({@code =}). If null or blank, {@link KvpMap#NULL_MAP} is
         *     returned.
         * @return a {@code KvpMap} containing the parsed key-value pairs
         */
        static KvpMap fromString(String name, String text) {
            return fromString(name, text, Split.on(';'), Split.on('='));
        }

        /**
         * Parses a string into a {@code KvpMap} using the provided {@code Splitter} instance for
         * splitting pairs and key-value components.
         *
         * @param name the name to be assigned to the created {@code KvpMap}
         * @param text the input string to be parsed. If null or blank, {@link KvpMap#NULL_MAP} is
         *     returned.
         * @param pairs a {@code Splitter} used to split the input string into pairs
         * @param keyValue a {@code Splitter} used to split each pair into key-value components
         * @return a {@code KvpMap} containing the parsed key-value pairs. If the input string is
         *     null or blank, {@link KvpMap#NULL_MAP} is returned.
         */
        static KvpMap fromString(String name, String text, Splitter pairs, Splitter keyValue) {
            if (text == null || text.isBlank()) {
                return KvpMap.NULL_MAP;
            }

            List<String> tokens = pairs.splitToList(text);
            Map<String, KvpValue> values = new LinkedHashMap<>();
            for (int i = 0; i < tokens.size(); i++) {
                String pairToBeSplitted = tokens.get(i);
                keyValue.splitToPair(pairToBeSplitted, true)
                        .ifPresent(
                                pair ->
                                        values.put(
                                                pair.key(),
                                                new KvpValue(pair.key(), pair.value())));
            }

            return new KvpMap(name, values);
        }
    }

    private static final class KvpNodeSelector extends StructuredBaseSelector<KvpNode>
            implements KeySelector<String>, ValueSelector<String> {

        private final BiFunction<String, String, KvpNode> kvpMapFactory;

        KvpNodeSelector(
                String name,
                ExtractionExpression expression,
                BiFunction<String, String, KvpNode> kvpMapFactory,
                Constant constant)
                throws ExtractionException {
            super(name, expression, constant);
            this.kvpMapFactory = kvpMapFactory;
        }

        @Override
        public Data extractKey(KafkaRecord<String, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(record::key, kvpMapFactory, checkScalar);
        }

        @Override
        public Data extractValue(KafkaRecord<?, String> record, boolean checkScalar)
                throws ValueException {
            return eval(record::value, kvpMapFactory, checkScalar);
        }
    }

    private static class KvpNodeSelectorSupplier {

        private final Deserializer<String> deserializer;
        protected final BiFunction<String, String, KvpNode> kvpMapFactory;

        KvpNodeSelectorSupplier(Splitter pair, Splitter keyValue) {
            this.deserializer = Serdes.String().deserializer();
            this.kvpMapFactory =
                    (rootName, payload) -> {
                        return KvpMap.fromString(rootName, payload, pair, keyValue);
                    };
        }

        public Deserializer<String> deserializer() {
            return deserializer;
        }
    }

    private static class KvpNodeValueSelectorSupplier extends KvpNodeSelectorSupplier
            implements ValueSelectorSupplier<String> {

        KvpNodeValueSelectorSupplier(Splitter pair, Splitter keyValue) {
            super(pair, keyValue);
        }

        @Override
        public ValueSelector<String> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new KvpNodeSelector(name, expression, kvpMapFactory, Constant.VALUE);
        }
    }

    private static class KvpNodeKeySelectorSupplier extends KvpNodeSelectorSupplier
            implements KeySelectorSupplier<String> {

        KvpNodeKeySelectorSupplier(Splitter pair, Splitter keyValue) {
            super(pair, keyValue);
        }

        @Override
        public KeySelector<String> newSelector(String name, ExtractionExpression expression)
                throws ExtractionException {
            return new KvpNodeSelector(name, expression, kvpMapFactory, Constant.KEY);
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
