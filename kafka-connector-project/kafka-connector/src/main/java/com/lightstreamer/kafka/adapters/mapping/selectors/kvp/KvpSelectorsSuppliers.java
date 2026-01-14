
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

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;

import com.lightstreamer.kafka.adapters.config.ConnectorConfig;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.Data;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelector;
import com.lightstreamer.kafka.common.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka.common.mapping.selectors.KeyValueSelectorSuppliersMaker;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node.NullNode;
import com.lightstreamer.kafka.common.mapping.selectors.SelectorEvaluatorType;
import com.lightstreamer.kafka.common.mapping.selectors.StructuredBaseSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelector;
import com.lightstreamer.kafka.common.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.utils.Split;
import com.lightstreamer.kafka.common.utils.Split.Splitter;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class KvpSelectorsSuppliers implements KeyValueSelectorSuppliersMaker<String> {

    interface KvpNode extends Node<KvpNode> {

        default boolean isArray() {
            return false;
        }

        @Override
        default int size() {
            return 0;
        }

        /**
         * Parses a string containing key-value pairs using the default delimiters.
         *
         * <p>The input string is expected to contain key-value pairs separated by a semicolon
         * ({@code ;}) and keys and values separated by an equals sign ({@code =}).
         *
         * @param name the name to be assigned to the created node
         * @param text the input string to be parsed. If null or blank, a {@code KvpNullNode} is
         *     returned.
         * @return a {@code KvpNode} - specifically, a {@code KvpMap} containing the parsed
         *     key-value pairs if the input is valid, or a {@code KvpNullNode} if the input is null
         *     or blank
         */
        static KvpNode fromString(String name, String text) {
            return fromString(name, text, Split.on(';'), Split.on('='));
        }

        /**
         * Parses a string containing key-value pairs using custom splitters.
         *
         * <p>This method allows customization of the delimiters used to split the input string. The
         * {@code pairs} splitter divides the input into individual key-value pair strings, and the
         * {@code keyValue} splitter separates each pair into its key and value components.
         *
         * @param name the name to be assigned to the created node
         * @param text the input string to be parsed. If null or blank, a {@code KvpNullNode} is
         *     returned.
         * @param pairs a {@code Splitter} used to split the input string into individual pair
         *     strings
         * @param keyValue a {@code Splitter} used to split each pair string into key and value
         *     components
         * @return a {@code KvpNode} - specifically, a {@code KvpMap} containing the parsed
         *     key-value pairs if the input is valid, or a {@code KvpNullNode} if the input is null
         *     or blank
         */
        static KvpNode fromString(String name, String text, Splitter pairs, Splitter keyValue) {
            if (text == null || text.isBlank()) {
                return new KvpNullNode(name);
            }

            List<String> tokens = pairs.splitToList(text);
            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < tokens.size(); i++) {
                keyValue.splitToPair(tokens.get(i), true)
                        .ifPresent(pair -> values.put(pair.key(), pair.value()));
            }

            return new KvpMap(name, values);
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
        public boolean isScalar() {
            return true;
        }

        @Override
        public KvpNode getProperty(String nodeName, String propertyName) {
            throw ValueException.scalarObject(propertyName);
        }

        @Override
        public KvpNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            throw ValueException.noIndexedField(indexedPropertyName);
        }
    }

    static class KvpNullNode extends NullNode<KvpNode> implements KvpNode {

        private KvpNullNode(String name) {
            super(name);
        }
    }

    static class KvpMap implements KvpNode {

        private final String name;
        private final Map<String, String> values;

        KvpMap(String name, Map<String, String> values) {
            this.name = name;
            this.values = values;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean isNull() {
            return values.isEmpty();
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
        public KvpNode getProperty(String nodeName, String propertyName) {
            String value = values.get(propertyName);
            if (value != null) {
                return new KvpValue(nodeName, value);
            }

            throw ValueException.fieldNotFound(propertyName);
        }

        @Override
        public KvpNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            throw ValueException.nonArrayObject(index);
        }

        @Override
        public String text() {
            return values.toString();
        }

        @Override
        public String toString() {
            return text();
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            target.putAll(values);
        }
    }

    private static final class KvpNodeSelector extends StructuredBaseSelector<String, KvpNode>
            implements KeySelector<String>, ValueSelector<String> {

        KvpNodeSelector(
                ExtractionExpression expression,
                BiFunction<String, String, KvpNode> kvpMapFactory,
                Constant constant)
                throws ExtractionException {
            super(expression, constant, kvpMapFactory);
        }

        @Override
        public Data extractKey(String name, KafkaRecord<String, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(name, record::key, checkScalar);
        }

        @Override
        public Data extractKey(KafkaRecord<String, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(record::key, checkScalar);
        }

        @Override
        public void extractKeyInto(KafkaRecord<String, ?> record, Map<String, String> target)
                throws ValueException {
            evalInto(record::key, target);
        }

        @Override
        public Data extractValue(String name, KafkaRecord<?, String> record, boolean checkScalar)
                throws ValueException {
            return eval(name, record::value, checkScalar);
        }

        @Override
        public Data extractValue(KafkaRecord<?, String> record, boolean checkScalar)
                throws ValueException {
            return eval(record::value, checkScalar);
        }

        @Override
        public void extractValueInto(KafkaRecord<?, String> record, Map<String, String> target)
                throws ValueException {
            evalInto(record::value, target);
        }
    }

    private abstract static sealed class KvpNodeSelectorSupplier
            permits KvpNodeKeySelectorSupplier, KvpNodeValueSelectorSupplier {

        private final Deserializer<String> deserializer;
        protected final BiFunction<String, String, KvpNode> kvpMapFactory;

        KvpNodeSelectorSupplier(Splitter pair, Splitter keyValue) {
            this.deserializer = Serdes.String().deserializer();
            this.kvpMapFactory =
                    (rootName, payload) -> {
                        return KvpNode.fromString(rootName, payload, pair, keyValue);
                    };
        }

        public final Deserializer<String> deserializer() {
            return deserializer;
        }

        public final SelectorEvaluatorType evaluatorType() {
            return EvaluatorType.KVP;
        }
    }

    private static final class KvpNodeKeySelectorSupplier extends KvpNodeSelectorSupplier
            implements KeySelectorSupplier<String> {

        KvpNodeKeySelectorSupplier(Splitter pair, Splitter keyValue) {
            super(pair, keyValue);
        }

        @Override
        public KeySelector<String> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new KvpNodeSelector(expression, kvpMapFactory, KEY);
        }
    }

    private static final class KvpNodeValueSelectorSupplier extends KvpNodeSelectorSupplier
            implements ValueSelectorSupplier<String> {

        KvpNodeValueSelectorSupplier(Splitter pair, Splitter keyValue) {
            super(pair, keyValue);
        }

        @Override
        public ValueSelector<String> newSelector(ExtractionExpression expression)
                throws ExtractionException {
            return new KvpNodeSelector(expression, kvpMapFactory, VALUE);
        }
    }

    private final ConnectorConfig config;

    public KvpSelectorsSuppliers(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public KeySelectorSupplier<String> makeKeySelectorSupplier() {
        if (!config.getKeyEvaluator().is(EvaluatorType.KVP)) {
            throw new IllegalArgumentException("Evaluator type is not KVP");
        }

        Splitter pair = Split.on(config.getKeyKvpPairsSeparator());
        Splitter keyValue = Split.on(config.getKeyKvpKeyValueSeparator());
        return new KvpNodeKeySelectorSupplier(pair, keyValue);
    }

    @Override
    public ValueSelectorSupplier<String> makeValueSelectorSupplier() {
        if (!config.getValueEvaluator().is(EvaluatorType.KVP)) {
            throw new IllegalArgumentException("Evaluator type is not KVP");
        }

        Splitter pair = Split.on(config.getValueKvpPairsSeparator());
        Splitter keyValue = Split.on(config.getValueKvpKeyValueSeparator());
        return new KvpNodeValueSelectorSupplier(pair, keyValue);
    }
}
