
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;

public class SchemaAndValueProvider {

    private SchemaAndValueProvider() {}

    public static Struct STRUCT = new SchemaAndValueProvider().newNode();

    private Struct newNode() {
        Schema grandSonsSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();

        Schema childrenSchema =
                SchemaBuilder.struct()
                        .field("name", Schema.STRING_SCHEMA)
                        .field("signature", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("children", SchemaBuilder.array(grandSonsSchema).optional().build())
                        .optional() // This allows to put null entries.
                        .build();

        // Schema preferencesSchema =
        //         SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
        // Schema documentsSchema =
        //         SchemaBuilder.map(
        //                         Schema.STRING_SCHEMA,
        //                         SchemaBuilder.struct()
        //                                 .field("doc_id", Schema.STRING_SCHEMA)
        //                                 .field("doc_type", Schema.STRING_SCHEMA)
        //                                 .build())
        //                 .build();

        Schema rootSchema =
                SchemaBuilder.struct()
                        .name("com.lightstreamer.kafka.connect.Value")
                        .field("name", Schema.STRING_SCHEMA)
                        .field("signature", Schema.OPTIONAL_BYTES_SCHEMA)
                        // .field("preferences", preferencesSchema)
                        // .field("documents", documentsSchema)
                        .field("children", SchemaBuilder.array(childrenSchema).optional().build())
                        .build();

        List<Object> joeChildren =
                new ArrayList<>(
                        List.of(
                                new Struct(childrenSchema).put("name", "alex"),
                                new Struct(childrenSchema)
                                        .put("name", "anna")
                                        .put(
                                                "children",
                                                List.of(
                                                        new Struct(grandSonsSchema)
                                                                .put("name", "gloria"),
                                                        new Struct(grandSonsSchema)
                                                                .put("name", "terence"))),
                                new Struct(childrenSchema).put("name", "serena")));
        joeChildren.add(null);
        Struct value =
                new Struct(rootSchema)
                        .put("name", "joe")
                        .put("signature", new byte[] {97, 98, 99, 100})
                        .put("children", joeChildren);

        return value;
    }
}
