
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

import com.lightstreamer.kafka.common.expressions.Constant;
import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeader;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeaders;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class HeadersSelectorSupplier implements SelectorSupplier<GenericSelector> {

    interface HeaderNode extends Node<HeaderNode> {

        static String toText(KafkaHeader header) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
    }

    static class SingleHeaderNode implements HeaderNode {

        private final KafkaHeader header;

        SingleHeaderNode(KafkaHeader header) {
            this.header = header;
        }

        @Override
        public String asText() {
            return HeaderNode.toText(header);
        }

        @Override
        public boolean isScalar() {
            return true;
        }
    }

    static class ArrayHeaderNode implements HeaderNode {

        private final List<KafkaHeader> headers;

        ArrayHeaderNode(List<KafkaHeader> headers) {
            this.headers = headers;
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public int size() {
            return headers.size();
        }

        @Override
        public Node<HeaderNode> get(int index) {
            return new SingleHeaderNode(headers.get(index));
        }

        @Override
        public String asText() {
            return headers.stream()
                    .map(HeaderNode::toText)
                    .collect(Collectors.joining(", ", "[", "]"));
        }
    }

    static class HeadersNode implements HeaderNode {

        private final KafkaHeaders headers;

        HeadersNode(KafkaHeaders headers) {
            this.headers = headers;
        }

        @Override
        public boolean has(String propertyname) {
            return headers.lastHeader(propertyname) != null;
        }

        @Override
        public Node<HeaderNode> get(String propertyname) {
            List<KafkaHeader> headersList = headers.headers(propertyname);
            return switch (headersList.size()) {
                case 0 -> Node.nullNode();
                case 1 -> new SingleHeaderNode(headersList.get(0));
                default -> new ArrayHeaderNode(headersList);
            };
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public int size() {
            return headers.size();
        }

        @Override
        public Node<HeaderNode> get(int index) {
            return new SingleHeaderNode(headers.get(index));
        }

        @Override
        public boolean isScalar() {
            return false;
        }

        @Override
        public String asText() {
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
            Node<HeaderNode> headersNode = Node.rootNode(() -> new HeadersNode(record.headers()));
            return super.eval(headersNode, checkScalar);
        }
    }

    @Override
    public GenericSelector newSelector(String name, ExtractionExpression expression)
            throws ExtractionException {
        return new HeadersSelectorImpl(name, expression);
    }
}
