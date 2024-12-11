
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
        value.family =
                new Value[][] {
                    {new Value("bro00"), new Value("bro01")},
                    {new Value("bro10"), new Value("bro11")}
                };

        ObjectNode node = new ObjectMapper().valueToTree(value);
        return node;
    }

    public static void main(String[] args) {
        // System.out.println(new JsonNodeProvider().newNode());

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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"name\":\"").append(this.name).append("\",");
        if (this.children != null) {
            sb.append("\"children\":");
            sb.append("[");
            for (int i = 0; i < this.children.size(); i++) {
                sb.append(this.children.get(i));
                if (i < this.children.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
        }
        if (this.family != null) {
            sb.append("\"family\":");
            sb.append("[");
            for (int i = 0; i < this.family.length; i++) {
                sb.append("[");
                for (int j = 0; j < this.family[i].length; j++) {
                    sb.append(this.family[i][j]);
                    if (j < this.family[i].length - 1) {
                        sb.append(",");
                    }
                }
                sb.append("]");
                if (i < this.family.length - 1) {
                    sb.append(",");
                }
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}
