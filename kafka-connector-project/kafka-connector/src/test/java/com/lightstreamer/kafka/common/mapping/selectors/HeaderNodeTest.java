
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

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.common.mapping.selectors.HeadersSelectorSupplier.ArrayHeaderNode;
import com.lightstreamer.kafka.common.mapping.selectors.HeadersSelectorSupplier.HeaderNode;
import com.lightstreamer.kafka.common.mapping.selectors.HeadersSelectorSupplier.HeadersNode;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaRecordHeaders;
import com.lightstreamer.kafka.common.mapping.selectors.Parsers.Node;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class HeaderNodeTest {

    @Test
    public void shouldCreateHeadersNode() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key", "value".getBytes(StandardCharsets.UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaRecordHeaders(headers));
        assertThat(headersNode.isArray()).isTrue();
        assertThat(headersNode.isScalar()).isFalse();
        assertThat(headersNode.isNull()).isFalse();
        assertThat(headersNode.size()).isEqualTo(1);
        assertThat(headersNode.has("key")).isTrue();
        assertThat(headersNode.has("non-existent-key")).isFalse();
        assertThat(headersNode.asText()).isEqualTo("{key=value}");
    }

    @Test
    public void shouldGetArrayNode() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key1", "value1ForKey1".getBytes(StandardCharsets.UTF_8));
        headers.add("key2", "value1ForKey2".getBytes(StandardCharsets.UTF_8));
        headers.add("key1", "value2ForKey1".getBytes(StandardCharsets.UTF_8));
        headers.add("key2", "value2ForKey2".getBytes(StandardCharsets.UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaRecordHeaders(headers));
        assertThat(headersNode.isArray()).isTrue();
        assertThat(headersNode.isScalar()).isFalse();
        assertThat(headersNode.isNull()).isFalse();
        assertThat(headersNode.size()).isEqualTo(4);
        assertThat(headersNode.asText())
                .isEqualTo(
                        "{key1=value1ForKey1, key2=value1ForKey2, key1=value2ForKey1, key2=value2ForKey2}");

        assertThat(headersNode.has("key1")).isTrue();
        Node<HeaderNode> arrayNodeForKey1 = headersNode.get("key1");
        assertThat(arrayNodeForKey1).isInstanceOf(ArrayHeaderNode.class);
        assertThat(arrayNodeForKey1.isArray()).isTrue();
        assertThat(arrayNodeForKey1.isScalar()).isFalse();
        assertThat(arrayNodeForKey1.isNull()).isFalse();
        assertThat(arrayNodeForKey1.size()).isEqualTo(2);
        assertThat(arrayNodeForKey1.asText()).isEqualTo("[value1ForKey1, value2ForKey1]");

        Node<HeaderNode> singleNode1 = arrayNodeForKey1.get(0);
        assertThat(singleNode1.isArray()).isFalse();
        assertThat(singleNode1.isScalar()).isTrue();
        assertThat(singleNode1.isNull()).isFalse();
        assertThat(singleNode1.asText()).isEqualTo("value1ForKey1");

        Node<HeaderNode> singleNode2 = arrayNodeForKey1.get(1);
        assertThat(singleNode2.isArray()).isFalse();
        assertThat(singleNode2.isScalar()).isTrue();
        assertThat(singleNode2.isNull()).isFalse();
        assertThat(singleNode2.asText()).isEqualTo("value2ForKey1");

        assertThat(headersNode.has("key2")).isTrue();
        Node<HeaderNode> arrayNodeForKey2 = headersNode.get("key2");
        assertThat(arrayNodeForKey2.isArray()).isTrue();
        assertThat(arrayNodeForKey2.isScalar()).isFalse();
        assertThat(arrayNodeForKey2.isNull()).isFalse();
        assertThat(arrayNodeForKey2.size()).isEqualTo(2);
        assertThat(arrayNodeForKey2.asText()).isEqualTo("[value1ForKey2, value2ForKey2]");

        Node<HeaderNode> singleNode1ForKey2 = arrayNodeForKey2.get(0);
        assertThat(singleNode1ForKey2.isArray()).isFalse();
        assertThat(singleNode1ForKey2.isScalar()).isTrue();
        assertThat(singleNode1ForKey2.isNull()).isFalse();
        assertThat(singleNode1ForKey2.asText()).isEqualTo("value1ForKey2");

        Node<HeaderNode> singleNode2ForKey2 = arrayNodeForKey2.get(1);
        assertThat(singleNode2ForKey2.isArray()).isFalse();
        assertThat(singleNode2ForKey2.isScalar()).isTrue();
        assertThat(singleNode2ForKey2.isNull()).isFalse();
        assertThat(singleNode2ForKey2.asText()).isEqualTo("value2ForKey2");
    }

    @Test
    public void shouldGetNullNodeFromAnyKeyInArrayNode() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key1", "value1ForKey1".getBytes(StandardCharsets.UTF_8));
        headers.add("key1", "value2ForKey1".getBytes(StandardCharsets.UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaRecordHeaders(headers));

        Node<HeaderNode> arrayNodeForKey1 = headersNode.get("key1");
        Node<HeaderNode> nonExistentNode = arrayNodeForKey1.get("non-existent-key");
        assertThat(nonExistentNode.isArray()).isFalse();
        assertThat(nonExistentNode.isScalar()).isTrue();
        assertThat(nonExistentNode.isNull()).isTrue();
    }

    @Test
    public void shouldGetNullNodeFromNonExistentKeyInHeadersNode() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaRecordHeaders(headers));

        Node<HeaderNode> headerNode = headersNode.get("non-existent-key");
        assertThat(headerNode.isArray()).isFalse();
        assertThat(headerNode.isScalar()).isTrue();
        assertThat(headerNode.isNull()).isTrue();
    }

    @Test
    public void shouldGetSingleNodeFromKey() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key", "value".getBytes(StandardCharsets.UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaRecordHeaders(headers));

        Node<HeaderNode> headerNode = headersNode.get("key");
        assertThat(headerNode.isArray()).isFalse();
        assertThat(headerNode.isScalar()).isTrue();
        assertThat(headerNode.isNull()).isFalse();
        assertThat(headerNode.asText()).isEqualTo("value");
    }

    @Test
    public void shouldGetSingleNodeFromIndex() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key", "value".getBytes(StandardCharsets.UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaRecordHeaders(headers));

        Node<HeaderNode> headerNode = headersNode.get(0);
        assertThat(headerNode.isArray()).isFalse();
        assertThat(headerNode.isScalar()).isTrue();
        assertThat(headerNode.isNull()).isFalse();
        assertThat(headerNode.asText()).isEqualTo("value");
    }

    @Test
    public void shouldGetNullNodeFromAnyKeyInSingleHeaderNode() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key", "value".getBytes(StandardCharsets.UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaRecordHeaders(headers));
        Node<HeaderNode> singleNode = headersNode.get("key");
        Node<HeaderNode> node = singleNode.get("non-existent-property");
        assertThat(node.isNull()).isTrue();
        assertThat(node.isScalar()).isTrue();
        assertThat(node.isArray()).isFalse();
        assertThat(node.size()).isEqualTo(0);
    }
}
