
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

package com.lightstreamer.kafka.common.mapping.selectors;

import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.HEADERS;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeader;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeaders;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HeadersSelectorSupplier implements SelectorSupplier<HeadersSelector> {

    interface HeaderNode extends Node<HeaderNode> {

        @Override
        default boolean has(String propertyName) {
            return false;
        }

        @Override
        default boolean isArray() {
            return true;
        }

        @Override
        default boolean isScalar() {
            return !isArray();
        }

        default Iterator<KafkaHeader> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        default void flatIntoMap(Map<String, String> target) {
            Iterator<KafkaHeader> iter = iterator();
            while (iter.hasNext()) {
                KafkaHeader header = iter.next();
                String key =
                        header.localIndex() == -1
                                ? header.key()
                                : header.key() + "[" + header.localIndex() + "]";
                target.put(key, HeaderNode.toText(header));
            }
        }

        static String toText(KafkaHeader header) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
    }

    static class SingleHeaderNode implements HeaderNode {

        private final KafkaHeader header;
        private String name = "";

        SingleHeaderNode(String name, KafkaHeader header) {
            this.name = name;
            this.header = header;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String text() {
            return HeaderNode.toText(header);
        }

        @Override
        public boolean isArray() {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void flatIntoMap(Map<String, String> target) {
            // Left empty on purpose
        }

        @Override
        public HeaderNode getProperty(String nodeName, String propertyName) {
            throw ValueException.scalarObject(propertyName);
        }

        @Override
        public HeaderNode getIndexed(String nodeName, int index, String indexedPropertyName) {
            throw ValueException.noIndexedField(indexedPropertyName);
        }
    }

    static class SubArrayHeaderNode implements HeaderNode {

        private final String name;
        private final List<KafkaHeader> headers;
        private final int size;

        SubArrayHeaderNode(String name, List<KafkaHeader> headers) {
            this.name = name;
            this.headers = headers;
            this.size = headers.size();
        }

        public String name() {
            return name;
        }

        @Override
        public int size() {
            return headers.size();
        }

        @Override
        public HeaderNode getIndexed(String nodeName, int index, String propertyName) {
            if (index < size) {
                KafkaHeader header = headers.get(index);
                return new SingleHeaderNode(nodeName, header);
            }
            throw ValueException.indexOfOutBounds(index);
        }

        @Override
        public Iterator<KafkaHeader> iterator() {
            return headers.iterator();
        }

        @Override
        public String text() {
            return headers.stream()
                    .map(HeaderNode::toText)
                    .collect(Collectors.joining(", ", "[", "]"));
        }

        public HeaderNode getProperty(String nodeName, String propertyName) {
            throw ValueException.arrayObject(propertyName);
        }
    }

    static class HeadersNode implements HeaderNode {

        private final String name;
        private final KafkaHeaders headers;
        private final int size;

        HeadersNode(String name, KafkaHeaders headers) {
            this.name = name;
            this.headers = headers;
            this.size = headers.size();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public boolean has(String propertyname) {
            return headers.has(propertyname);
        }

        @Override
        public HeaderNode getProperty(String nodeName, String propertyname) {
            List<KafkaHeader> headersList = headers.headers(propertyname);
            if (headersList == null) {
                throw ValueException.fieldNotFound(propertyname);
            }

            if (headersList.size() == 1) {
                return new SingleHeaderNode(nodeName, headersList.get(0));
            }

            return new SubArrayHeaderNode(nodeName, headersList);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public HeaderNode getIndexed(String nodeName, int index, String propertyName) {
            if (index < size) {
                KafkaHeader header = headers.get(index);
                return new SingleHeaderNode(nodeName, header);
            }
            throw ValueException.indexOfOutBounds(index);
        }

        @Override
        public Iterator<KafkaHeader> iterator() {
            return headers.iterator();
        }

        @Override
        public String text() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            Iterator<KafkaHeader> iter = headers.iterator();
            while (iter.hasNext()) {
                KafkaHeader header = iter.next();
                sb.append(header.key()).append("=").append(HeaderNode.toText(header));
                if (iter.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append("}");
            return sb.toString();
        }
    }

    private static class HeadersSelectorImpl
            extends StructuredBaseSelector<KafkaHeaders, HeaderNode> implements HeadersSelector {

        HeadersSelectorImpl(ExtractionExpression expression) throws ExtractionException {
            super(expression, HEADERS, HeadersNode::new);
        }

        @Override
        public Data extract(String name, KafkaRecord<?, ?> record, boolean checkScalar)
                throws ValueException {
            return eval(name, record::headers, checkScalar);
        }

        @Override
        public Data extract(KafkaRecord<?, ?> record, boolean checkScalar) throws ValueException {
            return eval(record::headers, checkScalar);
        }

        @Override
        public void extractInto(KafkaRecord<?, ?> record, Map<String, String> target)
                throws ValueException {
            evalInto(record::headers, target);
        }
    }

    @Override
    public HeadersSelector newSelector(ExtractionExpression expression) throws ExtractionException {
        return new HeadersSelectorImpl(expression);
    }
}
