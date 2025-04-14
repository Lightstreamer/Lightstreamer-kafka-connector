
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

package com.lightstreamer.kafka.adapters.mapping.selectors.protobuf;

import static com.google.common.truth.Truth.assertThat;

import com.google.protobuf.DynamicMessage;
import com.lightstreamer.example.Person;
import com.lightstreamer.example.Role;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.MapNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.MessageNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.ProtobufNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.RepeatedNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.ScalarNode;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ProtobufNodeTest {

    static DynamicMessage MESSAGE =
            SampleMessageProviders.SampleDynamicMessageProvider().sampleMessage();

    @Test
    public void shouldCreateMessageNode() {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        assertThat(personMessageNode.isArray()).isFalse();
        assertThat(personMessageNode.size()).isEqualTo(0);
        assertThat(personMessageNode.isNull()).isFalse();
        assertThat(personMessageNode.isScalar()).isFalse();
        assertThat(personMessageNode.has("name")).isTrue();
        assertThat(personMessageNode.has("lastName")).isFalse();
        assertThat(personMessageNode.asText())
                .isEqualTo(
                        """
            name: "joe"
            car {
              brand: "BMW"
            }
            phoneNumbers: "012345"
            phoneNumbers: "123456"
            friends {
              name: "mike"
            }
            friends {
              name: "john"
            }
            otherAddresses {
              key: "club"
              value {
                city: "Siracusa"
                zip: "96100"
              }
            }
            otherAddresses {
              key: "work"
              value {
                city: "Milan"
                zip: "20124"
              }
            }
            signature: "abcd"
            job: EMPLOYEE
            simpleRoleName: "architect"
            """);
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        FIELD,     VALUE
                        name,      joe
                        signature, abcd
                        email,     <EMPTY>
                        """)
    public void shouldGetStringScalarValue(String field, String value) {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        ProtobufNode fieldNode = personMessageNode.get(field);
        assertThat(fieldNode).isInstanceOf(ScalarNode.class);
        ScalarNode fieldScalarNode = (ScalarNode) fieldNode;
        assertThat(fieldScalarNode.isArray()).isFalse();
        assertThat(fieldScalarNode.size()).isEqualTo(0);
        assertThat(fieldScalarNode.isNull()).isFalse();
        assertThat(fieldScalarNode.isScalar()).isTrue();
        String expectedValue = "<EMPTY>".equals(value) ? "" : value;
        assertThat(fieldScalarNode.asText()).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            textBlock =
                    """
                        FIELD,       VALUE
                        mainAddress, <EMPTY>
                        car,         brand: "BMW"
                        """)
    public void shouldGetMessageValue(String field, String value) {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        ProtobufNode node = personMessageNode.get(field);
        assertThat(node).isInstanceOf(MessageNode.class);

        MessageNode fieldMessageNode = (MessageNode) node;
        assertThat(fieldMessageNode.isArray()).isFalse();
        assertThat(fieldMessageNode.size()).isEqualTo(0);
        assertThat(fieldMessageNode.isNull()).isFalse();
        assertThat(fieldMessageNode.isScalar()).isFalse();
        String expectedValue = "<EMPTY>".equals(value) ? "" : value + "\n";
        String asText = fieldMessageNode.asText();
        assertThat(asText).isEqualTo(expectedValue);
    }

    @Test
    public void shouldGetEnumValue() {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        ProtobufNode node = personMessageNode.get("job");
        assertThat(node).isInstanceOf(ScalarNode.class);

        ScalarNode scalarNode = (ScalarNode) node;
        assertThat(scalarNode.isArray()).isFalse();
        assertThat(scalarNode.size()).isEqualTo(0);
        assertThat(scalarNode.isNull()).isFalse();
        assertThat(scalarNode.isScalar()).isTrue();
        assertThat(scalarNode.asText()).isEqualTo("EMPLOYEE");
    }

    @Test
    public void shouldGetRepeatedScalarValues() {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        ProtobufNode phoneNumbersNode = personMessageNode.get("phoneNumbers");
        assertThat(phoneNumbersNode).isInstanceOf(RepeatedNode.class);

        RepeatedNode phoneNumbersRepeatedNode = (RepeatedNode) phoneNumbersNode;
        assertThat(phoneNumbersRepeatedNode.isArray()).isTrue();
        assertThat(phoneNumbersRepeatedNode.size()).isEqualTo(2);
        assertThat(phoneNumbersRepeatedNode.isNull()).isFalse();
        assertThat(phoneNumbersRepeatedNode.isScalar()).isFalse();
        assertThat(phoneNumbersRepeatedNode.asText())
                .isEqualTo(
                        """
            phoneNumbers: "012345"
            phoneNumbers: "123456"
            """);

        ProtobufNode elemNode1 = phoneNumbersRepeatedNode.get(0);
        assertThat(elemNode1).isInstanceOf(ScalarNode.class);
        ScalarNode elemScalarNode1 = (ScalarNode) elemNode1;
        assertThat(elemScalarNode1.asText()).isEqualTo("012345");

        ProtobufNode elemNode2 = phoneNumbersRepeatedNode.get(1);
        assertThat(elemNode2).isInstanceOf(ScalarNode.class);
        ScalarNode elemScalarNode2 = (ScalarNode) elemNode2;
        assertThat(elemScalarNode2.asText()).isEqualTo("123456");
    }

    @Test
    public void shouldGetRepeatedMessageValues() {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        ProtobufNode friends = personMessageNode.get("friends");
        assertThat(friends).isInstanceOf(RepeatedNode.class);

        RepeatedNode friendsRepeatedNode = (RepeatedNode) friends;
        assertThat(friendsRepeatedNode.isArray()).isTrue();
        assertThat(friendsRepeatedNode.size()).isEqualTo(2);
        assertThat(friendsRepeatedNode.isNull()).isFalse();
        assertThat(friendsRepeatedNode.isScalar()).isFalse();
        assertThat(friendsRepeatedNode.asText())
                .isEqualTo(
                        """
            friends {
              name: "mike"
            }
            friends {
              name: "john"
            }
            """);

        ProtobufNode elemNode1 = friendsRepeatedNode.get(0);
        assertThat(elemNode1).isInstanceOf(MessageNode.class);
        MessageNode elemMessageNode1 = (MessageNode) elemNode1;
        assertThat(elemMessageNode1.get("name").asText()).isEqualTo("mike");

        ProtobufNode elemNode2 = friendsRepeatedNode.get(1);
        assertThat(elemNode2).isInstanceOf(MessageNode.class);
        MessageNode elemMessageNode2 = (MessageNode) elemNode2;
        assertThat(elemMessageNode2.get("name").asText()).isEqualTo("john");
    }

    @Test
    public void shouldGetMappedMessageValues() {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        ProtobufNode otherAddressesNode = personMessageNode.get("otherAddresses");
        assertThat(otherAddressesNode).isInstanceOf(MapNode.class);
        MapNode otherAddressesMapNode = (MapNode) otherAddressesNode;
        assertThat(otherAddressesMapNode.isArray()).isFalse();
        assertThat(otherAddressesMapNode.size()).isEqualTo(0);
        assertThat(otherAddressesMapNode.isNull()).isFalse();
        assertThat(otherAddressesMapNode.isScalar()).isFalse();
        assertThat(otherAddressesNode.asText())
                .isEqualTo(
                        """
            otherAddresses {
              key: "club"
              value {
                city: "Siracusa"
                zip: "96100"
              }
            }
            otherAddresses {
              key: "work"
              value {
                city: "Milan"
                zip: "20124"
              }
            }
            """);

        ProtobufNode elemNode1 = otherAddressesMapNode.get("work");
        assertThat(elemNode1).isInstanceOf(MessageNode.class);
        MessageNode elemMessageNode1 = (MessageNode) elemNode1;
        assertThat(elemMessageNode1.get("city").asText()).isEqualTo("Milan");
        assertThat(elemMessageNode1.get("zip").asText()).isEqualTo("20124");

        ProtobufNode elemNode2 = otherAddressesMapNode.get("club");
        assertThat(elemNode2).isInstanceOf(MessageNode.class);
        MessageNode elemMessageNode2 = (MessageNode) elemNode2;
        assertThat(elemMessageNode2.get("city").asText()).isEqualTo("Siracusa");
        assertThat(elemMessageNode2.get("zip").asText()).isEqualTo("96100");
    }

    @Test
    public void shouldGetOneofValues() {
        MessageNode personMessageNode = new MessageNode(MESSAGE);
        ProtobufNode simpleRoleNameNode = personMessageNode.get("simpleRoleName");
        assertThat(simpleRoleNameNode).isInstanceOf(ScalarNode.class);
        ScalarNode simpleRoleNameScalarNode = (ScalarNode) simpleRoleNameNode;
        assertThat(simpleRoleNameScalarNode.isArray()).isFalse();
        assertThat(simpleRoleNameScalarNode.size()).isEqualTo(0);
        assertThat(simpleRoleNameScalarNode.isNull()).isFalse();
        assertThat(simpleRoleNameScalarNode.isScalar()).isTrue();
        assertThat(simpleRoleNameScalarNode.asText()).isEqualTo("Software Architect");

        DynamicMessage message =
                DynamicMessage.newBuilder(
                                Person.newBuilder()
                                        .setComplexRole(
                                                Role.newBuilder()
                                                        .setName("Head of Development")
                                                        .setScope("Engineering"))
                                        .build())
                        .build();
        MessageNode personNode2 = new MessageNode(message);
        ProtobufNode complexRoleNode = personNode2.get("complexRole");
        assertThat(complexRoleNode).isInstanceOf(MessageNode.class);
        MessageNode complexRoleMessageNode = (MessageNode) complexRoleNode;
        assertThat(complexRoleMessageNode.isArray()).isFalse();
        assertThat(complexRoleMessageNode.size()).isEqualTo(0);
        assertThat(complexRoleMessageNode.isNull()).isFalse();
        assertThat(complexRoleMessageNode.isScalar()).isFalse();
        assertThat(complexRoleMessageNode.get("name").asText()).isEqualTo("Head of Development");
        assertThat(complexRoleMessageNode.get("scope").asText()).isEqualTo("Engineering");
    }
}
