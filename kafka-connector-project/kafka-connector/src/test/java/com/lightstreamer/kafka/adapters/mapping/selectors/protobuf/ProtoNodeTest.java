
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

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.lightstreamer.example.Address;
import com.lightstreamer.example.Car;
import com.lightstreamer.example.Job;
import com.lightstreamer.example.Person;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.MapNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.MessageNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.ProtoNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.RepeatedNode;
import com.lightstreamer.kafka.adapters.mapping.selectors.protobuf.DynamicMessageSelectorSuppliers.ScalarNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ProtoNodeTest {

    static Person person =
            Person.newBuilder()
                    .setName("joe")
                    .setJob(Job.EMPLOYEE)
                    .addPhoneNumbers("012345")
                    .addPhoneNumbers("123456")
                    .addFriends(Person.newBuilder().setName("mike").build())
                    .addFriends(Person.newBuilder().setName("john").build())
                    .putOtherAddresses(
                            "work", Address.newBuilder().setCity("Milan").setZip("20124").build())
                    .putOtherAddresses(
                            "club",
                            Address.newBuilder().setCity("Siracusa").setZip("96100").build())
                    .setSignature(ByteString.copyFromUtf8("abcd"))
                    .setCar(Car.newBuilder().setBrand("BMW").build())
                    .build();

    static DynamicMessage message = DynamicMessage.newBuilder(person).build();

    @Test
    public void shouldCreateMessageNode() {
        MessageNode personNode = new MessageNode(message);
        assertThat(personNode.isArray()).isFalse();
        assertThat(personNode.size()).isEqualTo(0);
        assertThat(personNode.isNull()).isFalse();
        assertThat(personNode.isScalar()).isFalse();
        assertThat(personNode.has("name")).isTrue();
        assertThat(personNode.has("lastName")).isFalse();
        assertThat(personNode.asText())
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
        MessageNode personNode = new MessageNode(message);
        ProtoNode node = personNode.get(field);
        assertThat(node).isInstanceOf(ScalarNode.class);
        ScalarNode scalarNode = (ScalarNode) node;
        assertThat(scalarNode.isArray()).isFalse();
        assertThat(scalarNode.size()).isEqualTo(0);
        assertThat(scalarNode.isNull()).isFalse();
        assertThat(scalarNode.isScalar()).isTrue();
        String expectedValue = "<EMPTY>".equals(value) ? "" : value;
        assertThat(scalarNode.asText()).isEqualTo(expectedValue);
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
        MessageNode personNode = new MessageNode(message);
        ProtoNode node = personNode.get(field);
        assertThat(node).isInstanceOf(MessageNode.class);
        MessageNode messageNode = (MessageNode) node;
        assertThat(messageNode.isArray()).isFalse();
        assertThat(messageNode.size()).isEqualTo(0);
        assertThat(messageNode.isNull()).isFalse();
        assertThat(messageNode.isScalar()).isFalse();
        String expectedValue = "<EMPTY>".equals(value) ? "" : value + "\n";
        String asText = messageNode.asText();
        assertThat(asText).isEqualTo(expectedValue);
    }

    @Test
    public void shouldGetEnumValue() {
        MessageNode personNode = new MessageNode(message);
        ProtoNode node = personNode.get("job");
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
        MessageNode personNode = new MessageNode(message);
        ProtoNode phoneNumbers = personNode.get("phoneNumbers");
        assertThat(phoneNumbers).isInstanceOf(RepeatedNode.class);
        RepeatedNode phoneNumbersNode = (RepeatedNode) phoneNumbers;
        assertThat(phoneNumbersNode.isArray()).isTrue();
        assertThat(phoneNumbersNode.size()).isEqualTo(2);
        assertThat(phoneNumbersNode.isNull()).isFalse();
        assertThat(phoneNumbersNode.isScalar()).isFalse();
        assertThat(phoneNumbersNode.asText())
                .isEqualTo(
                        """
            phoneNumbers: "012345"
            phoneNumbers: "123456"
            """);

        ProtoNode elem1 = phoneNumbersNode.get(0);
        assertThat(elem1).isInstanceOf(ScalarNode.class);
        ScalarNode elemNode = (ScalarNode) elem1;
        assertThat(elemNode.asText()).isEqualTo("012345");

        ProtoNode elem2 = phoneNumbersNode.get(1);
        assertThat(elem2).isInstanceOf(ScalarNode.class);
        ScalarNode elemNode2 = (ScalarNode) elem2;
        assertThat(elemNode2.asText()).isEqualTo("123456");
    }

    @Test
    public void shouldGetRepeatedMessageValues() {
        MessageNode personNode = new MessageNode(message);
        ProtoNode friends = personNode.get("friends");
        assertThat(friends).isInstanceOf(RepeatedNode.class);
        RepeatedNode friendsNode = (RepeatedNode) friends;
        assertThat(friendsNode.isArray()).isTrue();
        assertThat(friendsNode.size()).isEqualTo(2);
        assertThat(friendsNode.isNull()).isFalse();
        assertThat(friendsNode.isScalar()).isFalse();
        assertThat(friendsNode.asText())
                .isEqualTo(
                        """
            friends {
              name: "mike"
            }
            friends {
              name: "john"
            }
            """);

        ProtoNode elem1 = friendsNode.get(0);
        assertThat(elem1).isInstanceOf(MessageNode.class);
        MessageNode elemNode = (MessageNode) elem1;
        assertThat(elemNode.get("name").asText()).isEqualTo("mike");

        ProtoNode elem2 = friendsNode.get(1);
        assertThat(elem2).isInstanceOf(MessageNode.class);
        MessageNode elemNode2 = (MessageNode) elem2;
        assertThat(elemNode2.get("name").asText()).isEqualTo("john");
    }

    @Test
    public void shouldGetMappedMessageValues() {
        MessageNode personNode = new MessageNode(message);
        ProtoNode otherAddresses = personNode.get("otherAddresses");
        assertThat(otherAddresses).isInstanceOf(MapNode.class);
        MapNode otherAddressesNode = (MapNode) otherAddresses;
        assertThat(otherAddressesNode.isArray()).isFalse();
        assertThat(otherAddressesNode.size()).isEqualTo(0);
        assertThat(otherAddressesNode.isNull()).isFalse();
        assertThat(otherAddressesNode.isScalar()).isFalse();
        assertThat(otherAddresses.asText())
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

        ProtoNode elem1 = otherAddressesNode.get("work");
        assertThat(elem1).isInstanceOf(MessageNode.class);
        MessageNode elemNode = (MessageNode) elem1;
        assertThat(elemNode.get("city").asText()).isEqualTo("Milan");
        assertThat(elemNode.get("zip").asText()).isEqualTo("20124");

        ProtoNode elem2 = otherAddressesNode.get("club");
        assertThat(elem2).isInstanceOf(MessageNode.class);
        MessageNode elemNode2 = (MessageNode) elem2;
        assertThat(elemNode2.get("city").asText()).isEqualTo("Siracusa");
        assertThat(elemNode2.get("zip").asText()).isEqualTo("96100");
    }
}
