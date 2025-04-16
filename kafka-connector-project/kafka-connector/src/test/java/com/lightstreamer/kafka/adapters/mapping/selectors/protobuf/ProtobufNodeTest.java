
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
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.MapFieldNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.MessageWrapperNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.ProtobufNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.RepeatedFieldNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.ScalarFieldNode;
import com.lightstreamer.kafka.test_utils.SampleMessageProviders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ProtobufNodeTest {

    static DynamicMessage MESSAGE =
            SampleMessageProviders.SampleDynamicMessageProvider().sampleMessage();

    @Test
    public void shouldCreateMessageWrapperNode() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        assertThat(personMessageWrapperNode.isArray()).isFalse();
        assertThat(personMessageWrapperNode.size()).isEqualTo(0);
        assertThat(personMessageWrapperNode.isNull()).isFalse();
        assertThat(personMessageWrapperNode.isScalar()).isFalse();
        assertThat(personMessageWrapperNode.has("name")).isTrue();
        assertThat(personMessageWrapperNode.has("lastName")).isFalse();
        assertThat(personMessageWrapperNode.asText())
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
              friends {
                name: "robert"
                signature: "abcd"
              }
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
                country {
                  name: "Italy"
                }
              }
            }
            indexedAddresses {
              key: 1
              value {
                city: "Rome"
              }
            }
            booleanAddresses {
              key: false
              value {
                city: "Florence"
              }
            }
            booleanAddresses {
              key: true
              value {
                city: "Turin"
              }
            }
            data {
              key: "data"
              value: -13.3
            }
            signature: "abcd"
            job: EMPLOYEE
            simpleRoleName: "Software Architect"
            any {
              type_url: "type.googleapis.com/Car"
              value: "\\n\\004FORD"
            }
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
    public void shouldGetStringScalarField(String field, String value) {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode fieldNode = personMessageWrapperNode.get(field);
        assertThat(fieldNode).isInstanceOf(ScalarFieldNode.class);

        ScalarFieldNode fieldScalarNode = (ScalarFieldNode) fieldNode;
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
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode node = personMessageWrapperNode.get(field);
        assertThat(node).isInstanceOf(MessageWrapperNode.class);

        MessageWrapperNode fieldMessageWrapperNode = (MessageWrapperNode) node;
        assertThat(fieldMessageWrapperNode.isArray()).isFalse();
        assertThat(fieldMessageWrapperNode.size()).isEqualTo(0);
        assertThat(fieldMessageWrapperNode.isNull()).isFalse();
        assertThat(fieldMessageWrapperNode.isScalar()).isFalse();
        String expectedValue = "<EMPTY>".equals(value) ? "" : value + "\n";
        String asText = fieldMessageWrapperNode.asText();
        assertThat(asText).isEqualTo(expectedValue);
    }

    @Test
    public void shouldGetEnumField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode node = personMessageWrapperNode.get("job");
        assertThat(node).isInstanceOf(ScalarFieldNode.class);

        ScalarFieldNode fieldEnumNode = (ScalarFieldNode) node;
        assertThat(fieldEnumNode.isArray()).isFalse();
        assertThat(fieldEnumNode.size()).isEqualTo(0);
        assertThat(fieldEnumNode.isNull()).isFalse();
        assertThat(fieldEnumNode.isScalar()).isTrue();
        assertThat(fieldEnumNode.asText()).isEqualTo("EMPLOYEE");
    }

    @Test
    public void shouldGetRepeatedScalarField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode phoneNumbersNode = personMessageWrapperNode.get("phoneNumbers");
        assertThat(phoneNumbersNode).isInstanceOf(RepeatedFieldNode.class);

        RepeatedFieldNode phoneNumbersRepeatedNode = (RepeatedFieldNode) phoneNumbersNode;
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
        assertThat(elemNode1).isInstanceOf(ScalarFieldNode.class);
        ScalarFieldNode elemScalarNode1 = (ScalarFieldNode) elemNode1;
        assertThat(elemScalarNode1.asText()).isEqualTo("012345");

        ProtobufNode elemNode2 = phoneNumbersRepeatedNode.get(1);
        assertThat(elemNode2).isInstanceOf(ScalarFieldNode.class);
        ScalarFieldNode elemScalarNode2 = (ScalarFieldNode) elemNode2;
        assertThat(elemScalarNode2.asText()).isEqualTo("123456");
    }

    @Test
    public void shouldGetRepeatedMessageField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode friends = personMessageWrapperNode.get("friends");
        assertThat(friends).isInstanceOf(RepeatedFieldNode.class);

        RepeatedFieldNode friendsRepeatedNode = (RepeatedFieldNode) friends;
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
              friends {
                name: "robert"
                signature: "abcd"
              }
            }
            """);

        ProtobufNode elemNode1 = friendsRepeatedNode.get(0);
        assertThat(elemNode1).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode elemMessageNode1 = (MessageWrapperNode) elemNode1;
        assertThat(elemMessageNode1.get("name").asText()).isEqualTo("mike");

        ProtobufNode elemNode2 = friendsRepeatedNode.get(1);
        assertThat(elemNode2).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode elemMessageNode2 = (MessageWrapperNode) elemNode2;
        assertThat(elemMessageNode2.get("name").asText()).isEqualTo("john");
    }

    @Test
    public void shouldGetMapOfStringKeyAndMessageValueField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode otherAddressesNode = personMessageWrapperNode.get("otherAddresses");
        assertThat(otherAddressesNode).isInstanceOf(MapFieldNode.class);

        MapFieldNode otherAddressesMapNode = (MapFieldNode) otherAddressesNode;
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
                country {
                  name: "Italy"
                }
              }
            }
                        """);

        ProtobufNode elemNode1 = otherAddressesMapNode.get("work");
        assertThat(elemNode1).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode elemMessageNode1 = (MessageWrapperNode) elemNode1;
        assertThat(elemMessageNode1.get("city").asText()).isEqualTo("Milan");
        assertThat(elemMessageNode1.get("zip").asText()).isEqualTo("20124");

        ProtobufNode elemNode2 = otherAddressesMapNode.get("club");
        assertThat(elemNode2).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode elemMessageNode2 = (MessageWrapperNode) elemNode2;
        assertThat(elemMessageNode2.get("city").asText()).isEqualTo("Siracusa");
        assertThat(elemMessageNode2.get("zip").asText()).isEqualTo("96100");
    }

    @Test
    public void shouldGetMapOfIntegralKeyAndMessageValueField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode otherAddressesNode = personMessageWrapperNode.get("indexedAddresses");
        assertThat(otherAddressesNode).isInstanceOf(MapFieldNode.class);

        MapFieldNode indexedAddressMapNode = (MapFieldNode) otherAddressesNode;
        assertThat(indexedAddressMapNode.isArray()).isFalse();
        assertThat(indexedAddressMapNode.size()).isEqualTo(0);
        assertThat(indexedAddressMapNode.isNull()).isFalse();
        assertThat(indexedAddressMapNode.isScalar()).isFalse();
        assertThat(otherAddressesNode.asText())
                .isEqualTo(
                        """
          indexedAddresses {
            key: 1
            value {
              city: "Rome"
            }
          }
          """);

        ProtobufNode elemNode1 = indexedAddressMapNode.get("1");
        assertThat(elemNode1).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode elemMessageNode1 = (MessageWrapperNode) elemNode1;
        assertThat(elemMessageNode1.get("city").asText()).isEqualTo("Rome");
    }

    @Test
    public void shouldGetMapOfBooleanKeyAndMessageValueField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode booleanAddressesNode = personMessageWrapperNode.get("booleanAddresses");
        assertThat(booleanAddressesNode).isInstanceOf(MapFieldNode.class);

        MapFieldNode indexedAddressMapNode = (MapFieldNode) booleanAddressesNode;
        assertThat(indexedAddressMapNode.isArray()).isFalse();
        assertThat(indexedAddressMapNode.size()).isEqualTo(0);
        assertThat(indexedAddressMapNode.isNull()).isFalse();
        assertThat(indexedAddressMapNode.isScalar()).isFalse();
        assertThat(booleanAddressesNode.asText())
                .isEqualTo(
                        """
          booleanAddresses {
            key: false
            value {
              city: "Florence"
            }
          }
          booleanAddresses {
            key: true
            value {
              city: "Turin"
            }
          }
          """);

        ProtobufNode elemNode1 = indexedAddressMapNode.get("true");
        assertThat(elemNode1).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode elemMessageNode1 = (MessageWrapperNode) elemNode1;
        assertThat(elemMessageNode1.get("city").asText()).isEqualTo("Turin");

        ProtobufNode elemNode2 = indexedAddressMapNode.get("false");
        assertThat(elemNode2).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode elemMessageNode2 = (MessageWrapperNode) elemNode2;
        assertThat(elemMessageNode2.get("city").asText()).isEqualTo("Florence");
    }

    @Test
    public void shouldGetMapOfStringKeyAndIntegralValueField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode dataNode = personMessageWrapperNode.get("data");
        assertThat(dataNode).isInstanceOf(MapFieldNode.class);

        MapFieldNode dataNapNode = (MapFieldNode) dataNode;
        assertThat(dataNapNode.isArray()).isFalse();
        assertThat(dataNapNode.size()).isEqualTo(0);
        assertThat(dataNapNode.isNull()).isFalse();
        assertThat(dataNapNode.isScalar()).isFalse();
        assertThat(dataNode.asText())
                .isEqualTo(
                        """
          data {
            key: "data"
            value: -13.3
          }
          """);

        ProtobufNode elemNode = dataNapNode.get("data");
        assertThat(elemNode).isInstanceOf(ScalarFieldNode.class);
        ScalarFieldNode elemMessageNode1 = (ScalarFieldNode) elemNode;
        assertThat(elemMessageNode1.asText()).isEqualTo("-13.3");
    }

    @Test
    public void shouldGetOneofField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode simpleRoleNameNode = personMessageWrapperNode.get("simpleRoleName");
        assertThat(simpleRoleNameNode).isInstanceOf(ScalarFieldNode.class);

        ScalarFieldNode simpleRoleNameScalarNode = (ScalarFieldNode) simpleRoleNameNode;
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
        MessageWrapperNode personNode2 = new MessageWrapperNode(message);
        ProtobufNode complexRoleNode = personNode2.get("complexRole");
        assertThat(complexRoleNode).isInstanceOf(MessageWrapperNode.class);

        MessageWrapperNode complexRoleMessageNode = (MessageWrapperNode) complexRoleNode;
        assertThat(complexRoleMessageNode.isArray()).isFalse();
        assertThat(complexRoleMessageNode.size()).isEqualTo(0);
        assertThat(complexRoleMessageNode.isNull()).isFalse();
        assertThat(complexRoleMessageNode.isScalar()).isFalse();
        assertThat(complexRoleMessageNode.get("name").asText()).isEqualTo("Head of Development");
        assertThat(complexRoleMessageNode.get("scope").asText()).isEqualTo("Engineering");
    }

    @Test
    public void shouldGetAnyField() {
        MessageWrapperNode personMessageWrapperNode = new MessageWrapperNode(MESSAGE);
        ProtobufNode anyNode = personMessageWrapperNode.get("any");
        assertThat(anyNode).isInstanceOf(MessageWrapperNode.class);
        MessageWrapperNode anyMessageNode = (MessageWrapperNode) anyNode;
        assertThat(anyMessageNode.isArray()).isFalse();
        assertThat(anyMessageNode.size()).isEqualTo(0);
        assertThat(anyMessageNode.isNull()).isFalse();
        assertThat(anyMessageNode.isScalar()).isFalse();
        assertThat(anyMessageNode.asText())
                .isEqualTo(
                        """
            type_url: "type.googleapis.com/Car"
            value: "\\n\\004FORD"
            """);

        ProtobufNode typeUrlNNode = anyMessageNode.get("type_url");
        assertThat(typeUrlNNode).isInstanceOf(ScalarFieldNode.class);
        ScalarFieldNode typeUrlScalarNode = (ScalarFieldNode) typeUrlNNode;
        assertThat(typeUrlScalarNode.isArray()).isFalse();
        assertThat(typeUrlScalarNode.size()).isEqualTo(0);
        assertThat(typeUrlScalarNode.isNull()).isFalse();
        assertThat(typeUrlScalarNode.isScalar()).isTrue();
        assertThat(typeUrlScalarNode.asText()).isEqualTo("type.googleapis.com/Car");

        ProtobufNode valueNode = anyMessageNode.get("value");
        assertThat(valueNode).isInstanceOf(ScalarFieldNode.class);
        ScalarFieldNode valueScalarNode = (ScalarFieldNode) valueNode;
        assertThat(valueScalarNode.isArray()).isFalse();
        assertThat(valueScalarNode.size()).isEqualTo(0);
        assertThat(valueScalarNode.isNull()).isFalse();
        assertThat(valueScalarNode.isScalar()).isTrue();
        assertThat(valueScalarNode.asText()).isEqualTo("\n\004FORD");
    }
}
