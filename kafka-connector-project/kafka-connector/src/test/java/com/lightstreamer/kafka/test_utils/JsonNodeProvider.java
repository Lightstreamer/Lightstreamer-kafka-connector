
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

package com.lightstreamer.kafka.test_utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public class JsonNodeProvider {

    private JsonNodeProvider() {}

    public static JsonNode RECORD = new JsonNodeProvider().newNode();

    private ObjectNode newNode() {
        List<Value> joeChildren =
                new ArrayList<>(
                        List.of(
                                new Value("alex"),
                                new Value(
                                        "anna", List.of(new Value("gloria"), new Value("terence"))),
                                new Value("serena")));
        joeChildren.add(null);

        Value value = new Value("joe", joeChildren);
        value.signature = new byte[] {97, 98, 99, 100};

        ObjectNode node = new ObjectMapper().valueToTree(value);
        return node;
    }
}

class Value {

    public String name;

    public Value child;

    public List<Value> children;

    public byte[] signature;

    public Value[][] family;

    public Value(String name) {
        this.name = name;
    }

    public Value(String name, List<Value> children) {
        this(name);
        this.children = children;
    }

    public Value(String name, Value[][] family) {
        this(name);
        this.family = family;
    }
}
