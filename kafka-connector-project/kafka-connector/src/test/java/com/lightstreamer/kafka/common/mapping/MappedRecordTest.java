
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.common.mapping.Items.subscribedFrom;

import static java.util.Collections.emptyMap;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.SchemaAndValues;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class MappedRecordTest {

    @Test
    public void shouldCreateSimpleMappedRecord() {
        SchemaAndValues expandedTemplate = SchemaAndValues.from("schema", Map.of("key", "aKey"));
        SchemaAndValues fieldsMap = SchemaAndValues.from("fields", Map.of("field1", "value1"));
        DefaultMappedRecord record = new DefaultMappedRecord(Set.of(expandedTemplate), fieldsMap);
        assertThat(record.expanded()).containsExactly(expandedTemplate);
        assertThat(record.fieldsMap()).containsExactly("field1", "value1");
        assertThat(record.toString())
                .isEqualTo(
                        "MappedRecord [expandedTemplates=[{key=aKey}], fieldsMap=(fields-<{field1=value1}>)]");
    }

    @Test
    public void shouldCreateMappedRecord() {
        Map<String, String> map1 = new LinkedHashMap<>();
        map1.put("key", "aKey");
        SchemaAndValues expandedTemplate1 = SchemaAndValues.from("schema1", map1);
        SchemaAndValues expandedTemplate2 = SchemaAndValues.from("schema2", emptyMap());

        Map<String, String> map2 = new LinkedHashMap<>();
        map2.put("value", "aValue");
        map2.put("partition", "aPartition");
        SchemaAndValues expandedTemplate3 = SchemaAndValues.from("schema3", map2);

        Set<SchemaAndValues> expandedTemplates = new LinkedHashSet<>();
        expandedTemplates.add(expandedTemplate1);
        expandedTemplates.add(expandedTemplate2);
        expandedTemplates.add(expandedTemplate3);

        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("field1", "value1");
        fields.put("field2", "value2");
        fields.put("field3", null);
        SchemaAndValues fieldsMap = SchemaAndValues.from("fields", fields);

        DefaultMappedRecord record = new DefaultMappedRecord(expandedTemplates, fieldsMap);
        assertThat(record.expanded())
                .containsExactly(expandedTemplate1, expandedTemplate2, expandedTemplate3);
        assertThat(record.fieldsMap())
                .containsExactly("field1", "value1", "field2", "value2", "field3", null);
        assertThat(record.toString())
                .isEqualTo(
                        "MappedRecord [expandedTemplates=[{key=aKey}, {}, {value=aValue, partition=aPartition}], fieldsMap=(fields-<{field1=value1, field2=value2, field3=null}>)]");
    }

    @Test
    public void shouldNotHaveExpandedTemplatesAndFieldsMapIfNOP() {
        assertThat(DefaultMappedRecord.NOPRecord.expanded()).isEmpty();
        assertThat(DefaultMappedRecord.NOPRecord.fieldsMap()).isEmpty();
        assertThat(DefaultMappedRecord.NOPRecord.toString())
                .isEqualTo("MappedRecord [expandedTemplates=[], fieldsMap=(NOSCHEMA-<{}>)]");
    }

    @Test
    public void shouldRouteMatchingParameterizedItems() {
        SchemaAndValues expandedTemplate1 =
                SchemaAndValues.from(
                        "schema1", Map.of("topic", "aTopic", "partition", "aPartition"));
        SchemaAndValues expandedTemplate2 =
                SchemaAndValues.from("schema2", Map.of("key", "aKey", "value", "aValue"));
        Set<SchemaAndValues> expandedTemplates = Set.of(expandedTemplate1, expandedTemplate2);
        DefaultMappedRecord record =
                new DefaultMappedRecord(expandedTemplates, SchemaAndValues.nop());

        // This item should match the expandedTemplate 1: routable
        SubscribedItem matchingItem1 =
                subscribedFrom("schema1-[topic=aTopic,partition=aPartition]");
        // This item should match the expandedTemplate 2: routable
        SubscribedItem matchingItem2 = subscribedFrom("schema2-[key=aKey,value=aValue]");
        // The following items should match no templates: non-routable
        SubscribedItem notMatchingBindParameters =
                subscribedFrom("schema1-[topic=anotherTopic,partition=anotherPartition]");
        SubscribedItem notMatchingSchema = subscribedFrom("schemaX-[key=aKey,value=aValue]");

        assertThat(
                        record.route(
                                Set.of(
                                        matchingItem1,
                                        matchingItem2,
                                        notMatchingBindParameters,
                                        notMatchingSchema)))
                .containsExactly(matchingItem1, matchingItem2);
        assertThat(record.route(Set.of(notMatchingBindParameters, notMatchingSchema))).isEmpty();
    }

    @Test
    public void shouldRouteMatchingSimpleItems() {
        SchemaAndValues expandedTemplate1 = SchemaAndValues.from("simple-item-1", emptyMap());
        SchemaAndValues expandedTemplate2 = SchemaAndValues.from("simple-item-2", emptyMap());
        DefaultMappedRecord record =
                new DefaultMappedRecord(
                        Set.of(expandedTemplate1, expandedTemplate2), SchemaAndValues.nop());
        assertThat(record.expanded()).containsExactly(expandedTemplate1, expandedTemplate2);
        assertThat(record.fieldsMap()).isEmpty();

        SubscribedItem matchingItem1 = subscribedFrom("simple-item-1");
        SubscribedItem matchingItem2 = subscribedFrom("simple-item-2");
        SubscribedItem notMatchingItem = subscribedFrom("simple-item-3");
        assertThat((record.route(Set.of(matchingItem1, matchingItem2, notMatchingItem))))
                .containsExactly(matchingItem1, matchingItem2);
    }
}
