
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class HeaderNodeTest {

    public void shouldCreateHeadersNode() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");
        Headers headers = record.headers();
        headers.add("key", "value".getBytes(UTF_8));

        HeadersNode headersNode =
                new HeadersSelectorSupplier.HeadersNode("HEADERS", new KafkaHeadersImpl(headers));
        assertThat(headersNode.name()).isEqualTo("HEADERS");
        assertThat(headersNode.isArray()).isTrue();
        assertThat(headersNode.isScalar()).isFalse();
        assertThat(headersNode.isNull()).isFalse();
        assertThat(headersNode.size()).isEqualTo(1);
        assertThat(headersNode.has("key")).isTrue();
        assertThat(headersNode.has("non-existent-key")).isFalse();
        assertThat(headersNode.getProperty("nodeKey1", "key").text()).isEqualTo("value");
        assertThat(headersNode.text()).isEqualTo("{key=value}");

        Map<String, String> target = new HashMap<>();
        headersNode.flatIntoMap(target);
        assertThat(target).hasSize(1);
        assertThat(target).containsEntry("key", "value");
    }

    static Stream<Arguments> multipleKeysHeaders() {
        return Stream.of(
                arguments(
                        new KafkaHeadersImpl(
                                new RecordHeaders()
                                        .add("key1", "value1ForKey1".getBytes(UTF_8))
                                        .add("key2", "value1ForKey2".getBytes(UTF_8))
                                        .add("key1", "value2ForKey1".getBytes(UTF_8))
                                        .add("key2", "value2ForKey2".getBytes(UTF_8)))),
                arguments(
                        new KafkaHeadersImpl(
                                new ConnectHeaders()
                                        .addBytes("key1", "value1ForKey1".getBytes(UTF_8))
                                        .addBytes("key2", "value1ForKey2".getBytes(UTF_8))
                                        .addBytes("key1", "value2ForKey1".getBytes(UTF_8))
                                        .addBytes("key2", "value2ForKey2".getBytes(UTF_8)))));
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldGetNodeFromMultipleKeysHeadersByKey(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        assertThat(headersNode.isArray()).isTrue();
        assertThat(headersNode.isScalar()).isFalse();
        assertThat(headersNode.isNull()).isFalse();
        assertThat(headersNode.size()).isEqualTo(4);
        assertThat(headersNode.text())
                .isEqualTo(
                        "{key1=value1ForKey1, key2=value1ForKey2, key1=value2ForKey1, key2=value2ForKey2}");

        assertThat(headersNode.has("key1")).isTrue();
        HeaderNode arrayNodeForKey1 = headersNode.getProperty("nodeKey1", "key1");
        assertThat(arrayNodeForKey1).isInstanceOf(SubArrayHeaderNode.class);
        assertThat(arrayNodeForKey1.name()).isEqualTo("nodeKey1");
        assertThat(arrayNodeForKey1.isArray()).isTrue();
        assertThat(arrayNodeForKey1.isScalar()).isFalse();
        assertThat(arrayNodeForKey1.isNull()).isFalse();
        assertThat(arrayNodeForKey1.size()).isEqualTo(2);
        assertThat(arrayNodeForKey1.text()).isEqualTo("[value1ForKey1, value2ForKey1]");

        HeaderNode singleNode1 = arrayNodeForKey1.getIndexed("singleNode1", 0, "arrayNode");
        assertThat(singleNode1).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode1.name()).isEqualTo("key1[0]");
        assertThat(singleNode1.isArray()).isFalse();
        assertThat(singleNode1.isScalar()).isTrue();
        assertThat(singleNode1.isNull()).isFalse();
        assertThat(singleNode1.text()).isEqualTo("value1ForKey1");

        HeaderNode singleNode2 = arrayNodeForKey1.getIndexed("singleNode2", 1, "arrayNode");
        assertThat(singleNode2).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode2.name()).isEqualTo("key1[1]");
        assertThat(singleNode2.isArray()).isFalse();
        assertThat(singleNode2.isScalar()).isTrue();
        assertThat(singleNode2.isNull()).isFalse();
        assertThat(singleNode2.text()).isEqualTo("value2ForKey1");

        assertThat(headersNode.has("key2")).isTrue();
        HeaderNode arrayNodeForKey2 = headersNode.getProperty("nodeKey2", "key2");
        assertThat(arrayNodeForKey2.name()).isEqualTo("nodeKey2");
        assertThat(arrayNodeForKey2.isArray()).isTrue();
        assertThat(arrayNodeForKey2.isScalar()).isFalse();
        assertThat(arrayNodeForKey2.isNull()).isFalse();
        assertThat(arrayNodeForKey2.size()).isEqualTo(2);
        assertThat(arrayNodeForKey2.text()).isEqualTo("[value1ForKey2, value2ForKey2]");

        HeaderNode singleNode1ForKey2 =
                arrayNodeForKey2.getIndexed("singleNode1ForKey2", 0, "arrayNodeForKey2");
        assertThat(singleNode1ForKey2).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode1ForKey2.name()).isEqualTo("key2[0]");
        assertThat(singleNode1ForKey2.isArray()).isFalse();
        assertThat(singleNode1ForKey2.isScalar()).isTrue();
        assertThat(singleNode1ForKey2.isNull()).isFalse();
        assertThat(singleNode1ForKey2.size()).isEqualTo(0);
        assertThat(singleNode1ForKey2.text()).isEqualTo("value1ForKey2");

        HeaderNode singleNode2ForKey2 =
                arrayNodeForKey2.getIndexed("singleNode2ForKey2", 1, "arrayNodeForKey2");
        assertThat(singleNode2ForKey2).isInstanceOf(SingleHeaderNode.class);
        assertThat(singleNode2ForKey2.name()).isEqualTo("key2[1]");
        assertThat(singleNode2ForKey2.isArray()).isFalse();
        assertThat(singleNode2ForKey2.isScalar()).isTrue();
        assertThat(singleNode2ForKey2.isNull()).isFalse();
        assertThat(singleNode2ForKey2.size()).isEqualTo(0);
        assertThat(singleNode2ForKey2.text()).isEqualTo("value2ForKey2");
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldNodesHaveIndexedName(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode node0 = headersNode.getIndexed("node0", 0, "headersNode");
        assertThat(node0).isInstanceOf(SingleHeaderNode.class);
        assertThat(node0.name()).isEqualTo("key1[0]");

        HeaderNode node1 = headersNode.getIndexed("node1", 1, "headersNode");
        assertThat(node1).isInstanceOf(SingleHeaderNode.class);
        assertThat(node1.name()).isEqualTo("key2[0]");

        HeaderNode node2 = headersNode.getIndexed("node2", 2, "headersNode");
        assertThat(node2).isInstanceOf(SingleHeaderNode.class);
        assertThat(node2.name()).isEqualTo("key1[1]");

        HeaderNode node3 = headersNode.getIndexed("node3", 3, "headersNode");
        assertThat(node3).isInstanceOf(SingleHeaderNode.class);
        assertThat(node3.name()).isEqualTo("key2[1]");
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
        assertThat(ve).hasMessageThat().isEqualTo("Field [any-key] not found");
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldNotGetByNameFromSingleHeaderNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode singleNode = headersNode.getProperty("nodeKey", "key");
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
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode singleNode = headersNode.getProperty("nodeKey", "key");
        assertThat(singleNode).isInstanceOf(SingleHeaderNode.class);
        ValueException ve =
                assertThrows(
                        ValueException.class, () -> singleNode.getIndexed("anyNode", 0, "nodeKey"));
        assertThat(ve).hasMessageThat().isEqualTo("Field [key] is not indexed");
    }

    static Stream<Arguments> emptyHeaders() {
        return Stream.of(
                arguments(
                        new KafkaHeadersImpl(new RecordHeaders()),
                        arguments(new KafkaHeadersImpl(new ConnectHeaders()))));
    }

    @ParameterizedTest
    @MethodSource("emptyHeaders")
    public void shouldGetNullFromNonExistentKeyInHeadersNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        ValueException ve =
                assertThrows(
                        ValueException.class,
                        () -> headersNode.getProperty("nodeNonExistent", "non-existent-key"));
        assertThat(ve).hasMessageThat().isEqualTo("Field [non-existent-key] not found");
    }

    static Stream<Arguments> singleKeyHeaders() {
        return Stream.of(
                arguments(
                        new KafkaHeadersImpl(
                                new RecordHeaders().add("key", "value".getBytes(UTF_8))),
                        arguments(
                                new KafkaHeadersImpl(
                                        new ConnectHeaders()
                                                .addBytes("key", "value".getBytes(UTF_8))))));
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldGetSingleHeaderNodeFromSingleKeyHeadersByKey(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);

        HeaderNode headerNode = headersNode.getProperty("nodeKey", "key");
        assertThat(headerNode).isInstanceOf(SingleHeaderNode.class);
        assertThat(headerNode.name()).isEqualTo("key");
        assertThat(headerNode.isArray()).isFalse();
        assertThat(headerNode.isScalar()).isTrue();
        assertThat(headerNode.isNull()).isFalse();
        assertThat(headerNode.size()).isEqualTo(0);
        assertThat(headerNode.text()).isEqualTo("value");
    }

    @ParameterizedTest
    @MethodSource("singleKeyHeaders")
    public void shouldGetSingleHeaderNodeFromSingleKeyHeadersByIndex(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);

        HeaderNode headerNode = headersNode.getIndexed("node0", 0, "headersNode");
        assertThat(headerNode).isInstanceOf(SingleHeaderNode.class);
        assertThat(headerNode.name()).isEqualTo("key");
        assertThat(headerNode.isArray()).isFalse();
        assertThat(headerNode.isScalar()).isTrue();
        assertThat(headerNode.isNull()).isFalse();
        assertThat(headerNode.size()).isEqualTo(0);
        assertThat(headerNode.text()).isEqualTo("value");
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
        assertThat(target).hasSize(2);
        assertThat(target)
                .containsExactly(
                        "key1[0]", "value1ForKey1",
                        "key1[1]", "value2ForKey1");
    }

    @ParameterizedTest
    @MethodSource("multipleKeysHeaders")
    public void shouldFlatIntoMapFromSingleHeaderNode(KafkaHeaders headers) {
        HeadersNode headersNode = new HeadersSelectorSupplier.HeadersNode("HEADERS", headers);
        HeaderNode node = headersNode.getProperty("nodeKey1", "key1");
        Map<String, String> target = new HashMap<>();
        node.flatIntoMap(target);
        assertThat(target).containsExactly("key1[0]", "value1ForKey1");
    }
}
