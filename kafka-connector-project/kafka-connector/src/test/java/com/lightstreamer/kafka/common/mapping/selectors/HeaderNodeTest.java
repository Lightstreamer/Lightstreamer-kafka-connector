
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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.lightstreamer.kafka.common.mapping.selectors.HeadersSelectorSupplier.HeaderNode;
import com.lightstreamer.kafka.common.mapping.selectors.HeadersSelectorSupplier.HeadersNode;
import com.lightstreamer.kafka.common.mapping.selectors.HeadersSelectorSupplier.SingleHeaderNode;
import com.lightstreamer.kafka.common.mapping.selectors.HeadersSelectorSupplier.SubArrayHeaderNode;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeaders;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord.KafkaHeadersImpl;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class HeaderNodeTest {

    @Test
    public void shouldCreateHeadersNode() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key", "value".getBytes(UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode(new KafkaConsumerRecordHeaders(headers));
        assertThat(headersNode.isArray()).isTrue();
        assertThat(headersNode.isScalar()).isFalse();
        assertThat(headersNode.isNull()).isFalse();
        assertThat(headersNode.size()).isEqualTo(1);
        assertThat(headersNode.has("key")).isTrue();
        assertThat(headersNode.has("non-existent-key")).isFalse();
        assertThat(headersNode.text()).isEqualTo("{key=value}");

        HeaderNode nodeKey1 = headersNode.getProperty("nodeKey1", "key");
        assertThat(nodeKey1.name()).isEqualTo("nodeKey1");
        assertThat(nodeKey1.text()).isEqualTo("value");

        Map<String, String> target = new HashMap<>();
        headersNode.flatIntoMap(target);
        assertThat(target).hasSize(1);
        assertThat(target).containsEntry("key", "value");
    }

    static Stream<KafkaHeaders> multipleKeysHeaders() {
        return Stream.of(
                arguments(
                        new KafkaConsumerRecordHeaders(
                                new RecordHeaders()
                                        .add("key1", "value1ForKey1".getBytes(UTF_8))
                                        .add("key2", "value1ForKey2".getBytes(UTF_8))
                                        .add("key1", "value2ForKey1".getBytes(UTF_8))
                                        .add("key2", "value2ForKey2".getBytes(UTF_8))),
                        arguments(
                                new KafkaConnectHeaders(
                                        new ConnectHeaders()
                                                .addBytes("key1", "value1ForKey1".getBytes(UTF_8))
                                                .addBytes("key2", "value1ForKey2".getBytes(UTF_8))
                                                .addBytes("key1", "value2ForKey1".getBytes(UTF_8))
                                                .addBytes(
                                                        "key2",
                                                        "value2ForKey2".getBytes(UTF_8))))));
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldGetNodeFromMultipleKeysHeadersByKey(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersNode("HEADERS", headers);
        assertThat(headersNode.isArray()).isTrue();
        assertThat(headersNode.isScalar()).isFalse();
        assertThat(headersNode.isNull()).isFalse();
        assertThat(headersNode.size()).isEqualTo(4);
        assertThat(headersNode.text())
                .isEqualTo(
                        "{key1=value1ForKey1, key2=value1ForKey2, key1=value2ForKey1, key2=value2ForKey2}");
        Map<String, String> target = new HashMap<>();
        headersNode.flatIntoMap(target);
        assertThat(target)
                .containsExactly(
                        "key1[0]", "value1ForKey1",
                        "key2[0]", "value1ForKey2",
                        "key1[1]", "value2ForKey1",
                        "key2[1]", "value2ForKey2");
        target.clear();

        assertThat(headersNode.has("key1")).isTrue();
        HeaderNode arrayNodeForKey1 = headersNode.getProperty("arrayNodeForKey1", "key1");
        assertThat(arrayNodeForKey1).isInstanceOf(SubArrayHeaderNode.class);
        assertThat(arrayNodeForKey1.name()).isEqualTo("arrayNodeForKey1");
        assertThat(arrayNodeForKey1.isArray()).isTrue();
        assertThat(arrayNodeForKey1.isScalar()).isFalse();
        assertThat(arrayNodeForKey1.isNull()).isFalse();
        assertThat(arrayNodeForKey1.size()).isEqualTo(2);
        assertThat(arrayNodeForKey1.text()).isEqualTo("[value1ForKey1, value2ForKey1]");

        HeaderNode singleNode1 = arrayNodeForKey1.getIndexed("singleNode1ForKey1", 0, "arrayNode");
        assertThat(singleNode1).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode1.name()).isEqualTo("singleNode1ForKey1");
        assertThat(singleNode1.isArray()).isFalse();
        assertThat(singleNode1.isScalar()).isTrue();
        assertThat(singleNode1.isNull()).isFalse();
        assertThat(singleNode1.text()).isEqualTo("value1ForKey1");

        HeaderNode singleNode2 = arrayNodeForKey1.getIndexed("singleNode2ForKey1", 1, "arrayNode");
        assertThat(singleNode2).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode2.name()).isEqualTo("singleNode2ForKey1");
        assertThat(singleNode2.isArray()).isFalse();
        assertThat(singleNode2.isScalar()).isTrue();
        assertThat(singleNode2.isNull()).isFalse();
        assertThat(singleNode2.text()).isEqualTo("value2ForKey1");

        assertThat(headersNode.has("key2")).isTrue();
        HeaderNode arrayNodeForKey2 = headersNode.getProperty("arrayNodeForKey2", "key2");
        assertThat(arrayNodeForKey2.name()).isEqualTo("arrayNodeForKey2");
        assertThat(arrayNodeForKey2.isArray()).isTrue();
        assertThat(arrayNodeForKey2.isScalar()).isFalse();
        assertThat(arrayNodeForKey2.isNull()).isFalse();
        assertThat(arrayNodeForKey2.size()).isEqualTo(2);
        assertThat(arrayNodeForKey2.text()).isEqualTo("[value1ForKey2, value2ForKey2]");

        HeaderNode singleNode1ForKey2 =
                arrayNodeForKey2.getIndexed("singleNode1ForKey2", 0, "arrayNodeForKey2");
        assertThat(singleNode1ForKey2).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode1ForKey2.name()).isEqualTo("singleNode1ForKey2");
        assertThat(singleNode1ForKey2.isArray()).isFalse();
        assertThat(singleNode1ForKey2.isScalar()).isTrue();
        assertThat(singleNode1ForKey2.isNull()).isFalse();
        assertThat(singleNode1ForKey2.size()).isEqualTo(0);
        assertThat(singleNode1ForKey2.text()).isEqualTo("value1ForKey2");

        HeaderNode singleNode2ForKey2 =
                arrayNodeForKey2.getIndexed("singleNode2ForKey2", 1, "arrayNodeForKey2");
        assertThat(singleNode2ForKey2).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode2ForKey2.name()).isEqualTo("singleNode2ForKey2");
        assertThat(singleNode2ForKey2.isArray()).isFalse();
        assertThat(singleNode2ForKey2.isScalar()).isTrue();
        assertThat(singleNode2ForKey2.isNull()).isFalse();
        assertThat(singleNode2ForKey2.size()).isEqualTo(0);
        assertThat(singleNode2ForKey2.text()).isEqualTo("value2ForKey2");
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldNotGetByNameFromArrayHeaderNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode arrayNodeForKey1 = headersNode.getProperty("nodeKey1", "key1");
        assertThat(arrayNodeForKey1).isInstanceOf(SubArrayHeaderNode.class);
        assertThat(arrayNodeForKey1.has("any-key")).isFalse();
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> arrayNodeForKey1.getProperty("anyNode", "any-key"));
        assertThat(ve)
                .hasMessageThat()
                .isEqualTo("Cannot retrieve field [any-key] from an array object");
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldNotFoundByIndexFromArrayHeaderNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode arrayNodeForKey1 = headersNode.getProperty("nodeKey1", "key1");
        assertThat(arrayNodeForKey1).isInstanceOf(SubArrayHeaderNode.class);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> arrayNodeForKey1.getIndexed("anyNode", 2, "arrayNode"));
        assertThat(ve).hasMessageThat().isEqualTo("Field not found at index [2]");
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldNotFoundByIndexFromSingleNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode singleNode = headersNode.getIndexed("nodeKey1", 0, "");
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> singleNode.getIndexed("anyNode", 0, "singleNode"));

        assertThat(ve).hasMessageThat().isEqualTo("Field [singleNode] is not indexed");
    }

    static Stream<Arguments> emptyHeaders() {
        return Stream.of(
                arguments(
                        new KafkaConsumerRecordHeaders(new RecordHeaders()),
                        arguments(new KafkaConnectHeaders(new ConnectHeaders()))));
    }

    @ParameterizedTest
    @MethodSource("emptyHeaders")
    public void shouldNotFoundNonExistingProperty(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> headersNode.getProperty("nodeNonExistent", "non-existent-key"));
        assertThat(ve).hasMessageThat().isEqualTo("Field [non-existent-key] not found");
    }

    static Stream<KafkaHeaders> singleKeyHeaders() {
        return Stream.of(
                new KafkaHeadersImpl(
                        new RecordHeaders()
                                .add("key1", "value1".getBytes(UTF_8))
                                .add("key2", "value2".getBytes(UTF_8))),
                new KafkaHeadersImpl(
                        new ConnectHeaders()
                                .addBytes("key1", "value1".getBytes(UTF_8))
                                .addBytes("key2", "value2".getBytes(UTF_8))));
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldNotGetByNameFromSingleHeaderNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode singleNode = headersNode.getProperty("nodeKey", "key1");
        assertThat(singleNode).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode.has("any-key")).isFalse();
        ValueException ve =
                assertThrows(
                        ValueException.class, () -> singleNode.getProperty("anyNode", "any-key"));
        assertThat(ve)
                .hasMessageThat()
                .isEqualTo("Cannot retrieve field [any-key] from a scalar object");
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldNotGetByIndexFromSingleHeaderNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersNode("HEADERS", headers);
        HeaderNode singleNode = headersNode.getProperty("nodeKey", "key1");
        assertThat(singleNode).isInstanceOf(SingleHeaderNode.class);
        ValueException ve =
                assertThrows(
                        ValueException.class, () -> singleNode.getIndexed("anyNode", 0, "nodeKey"));
        assertThat(ve).hasMessageThat().isEqualTo("Field [nodeKey] is not indexed");
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldGetSingleHeaderNodeFromSingleKeyHeadersByKey(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);

        HeaderNode nodeKey1 = headersNode.getProperty("nodeKey1", "key1");
        assertThat(nodeKey1).isInstanceOf(SingleHeaderNode.class);
        assertThat(nodeKey1.name()).isEqualTo("nodeKey1");
        assertThat(nodeKey1.isArray()).isFalse();
        assertThat(nodeKey1.isScalar()).isTrue();
        assertThat(nodeKey1.isNull()).isFalse();
        assertThat(nodeKey1.size()).isEqualTo(0);
        assertThat(nodeKey1.text()).isEqualTo("value1");

        HeaderNode nodeKey2 = headersNode.getProperty("nodeKey2", "key2");
        assertThat(nodeKey2).isInstanceOf(SingleHeaderNode.class);
        assertThat(nodeKey2.name()).isEqualTo("nodeKey2");
        assertThat(nodeKey2.isArray()).isFalse();
        assertThat(nodeKey2.isScalar()).isTrue();
        assertThat(nodeKey2.isNull()).isFalse();
        assertThat(nodeKey2.size()).isEqualTo(0);
        assertThat(nodeKey2.text()).isEqualTo("value2");
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldGetSingleHeaderNodeFromSingleKeyHeadersByIndex(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);

        HeaderNode node1 = headersNode.getIndexed("node1", 0, "headersNode");
        assertThat(node1).isInstanceOf(SingleHeaderNode.class);
        assertThat(node1.name()).isEqualTo("node1");
        assertThat(node1.isArray()).isFalse();
        assertThat(node1.isScalar()).isTrue();
        assertThat(node1.isNull()).isFalse();
        assertThat(node1.size()).isEqualTo(0);
        assertThat(node1.text()).isEqualTo("value1");

        HeaderNode node2 = headersNode.getIndexed("node2", 1, "headersNode");
        assertThat(node2).isInstanceOf(SingleHeaderNode.class);
        assertThat(node2.name()).isEqualTo("node2");
        assertThat(node2.isArray()).isFalse();
        assertThat(node2.isScalar()).isTrue();
        assertThat(node2.isNull()).isFalse();
        assertThat(node2.size()).isEqualTo(0);
        assertThat(node2.text()).isEqualTo("value2");
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldFlatIntoMapFromHeadersNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);

        Map<String, String> target = new HashMap<>();
        headersNode.flatIntoMap(target);
        assertThat(target).hasSize(4);
        assertThat(target)
                .containsExactly(
                        "key1[0]", "value1ForKey1",
                        "key2[0]", "value1ForKey2",
                        "key1[1]", "value2ForKey1",
                        "key2[1]", "value2ForKey2");
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldFlatIntoMapFromSubArrayNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);

        HeaderNode arrayNodeForKey1 = headersNode.getProperty("nodeKey1", "key1");
        Map<String, String> target = new HashMap<>();
        arrayNodeForKey1.flatIntoMap(target);
        assertThat(target)
                .containsExactly(
                        "key1[0]", "value1ForKey1",
                        "key1[1]", "value2ForKey1");
        target.clear();

        HeaderNode arrayNodeForKey2 = headersNode.getProperty("nodeKey2", "key2");
        arrayNodeForKey2.flatIntoMap(target);
        assertThat(target).hasSize(2);
        assertThat(target)
                .containsExactly(
                        "key2[0]", "value1ForKey2",
                        "key2[1]", "value2ForKey2");
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldNotFlatIntoMapFromSingleHeaderNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode nodeKey1 = headersNode.getProperty("nodeKey1", "key1");
        Map<String, String> target = new HashMap<>();
        nodeKey1.flatIntoMap(target);
        assertThat(target).isEmpty();
    }
}
