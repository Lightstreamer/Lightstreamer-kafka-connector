
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

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeader;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeaders;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HeadersSelectorSupplier implements SelectorSupplier<GenericSelector> {

    interface HeaderNode extends Node<HeaderNode> {

        default HeaderNode get(String propertyName) {
            return null;
        }

        default boolean has(String propertyName) {
            return false;
        }

        default HeaderNode get(int index) {
            return null;
        }

        default boolean isArray() {
            return true;
        }

        default boolean isScalar() {
            return !isArray();
        }

        default Iterator<KafkaHeader> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        default void visit(Consumer<Data> visitor) {
            Iterator<KafkaHeader> iter = iterator();
            while (iter.hasNext()) {
                KafkaHeader header = iter.next();
                visitor.accept(Data.from(header.key(), HeaderNode.toText(header)));
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
        public void visit(Consumer<Data> visitor) {
            visitor.accept(Data.from(header.key(), text()));
        }
    }

    static class SubArrayHeaderNode implements HeaderNode {

        private final List<KafkaHeader> headers;
        private final String name;

        SubArrayHeaderNode(String name, List<KafkaHeader> headers) {
            this.name = name;
            this.headers = headers;
        }

        public String name() {
            return name;
        }

        @Override
        public int size() {
            return headers.size();
        }

        @Override
        public void visit(Consumer<Data> visitor) {
            Iterator<KafkaHeader> iter = headers.iterator();
            while (iter.hasNext()) {
                KafkaHeader header = iter.next();
                visitor.accept(Data.from(header.key(), HeaderNode.toText(header)));
            }
        }

        @Override
        public HeaderNode get(int index) {
            return new SingleHeaderNode(name + "[" + index + "]", headers.get(index));
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
    }

    static class HeadersNode implements HeaderNode {

        private final KafkaHeaders headers;
        private final String name;

        HeadersNode(String name, KafkaHeaders headers) {
            this.name = name;
            this.headers = headers;
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
        public HeaderNode get(String propertyname) {
            List<KafkaHeader> headersList = headers.headers(propertyname);
            if (headersList == null) {
                return null;
            }

            if (headersList.size() == 1) {
                return new SingleHeaderNode(propertyname, headersList.get(0));
            }

            return new SubArrayHeaderNode(propertyname, headersList);
        }

        @Override
        public int size() {
            return headers.size();
        }

        @Override
        public HeaderNode get(int index) {
            KafkaHeader header = headers.get(index);
            String name =
                    header.localIndex() == -1
                            ? header.key()
                            : header.key() + "[" + header.localIndex() + "]";
            return new SingleHeaderNode(name, header);
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

    private static class HeadersSelectorImpl extends StructuredBaseSelector<HeaderNode>
            implements GenericSelector {

        HeadersSelectorImpl(String name, ExtractionExpression expression)
                throws ExtractionException {
            super(name, expression, Constant.HEADERS);
        }

        @Override
        public Data extract(KafkaRecord<?, ?> record, boolean checkScalar) throws ValueException {
            return eval(record::headers, HeadersNode::new, checkScalar);
        }

        @Override
        public Collection<Data> extractMulti(KafkaRecord<?, ?> record) throws ValueException {
            return evalMulti(record::headers, HeadersNode::new);
        }
    }

    @Override
    public GenericSelector newSelector(String name, ExtractionExpression expression)
            throws ExtractionException {
        return new HeadersSelectorImpl(name, expression);
    }
}
