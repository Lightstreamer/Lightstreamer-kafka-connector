
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

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MappedRecordTest {

    @Test
    public void shouldCreateSimpleMappedRecord() {
        String[] canonicalItemNames = {"schema-[key=aKey]"};
        Map<String, String> fieldsMap = Map.of("field1", "value1");

        DefaultMappedRecord record = new DefaultMappedRecord(canonicalItemNames, () -> fieldsMap);
        assertThat(record.fieldsMap()).containsExactly("field1", "value1");
        assertThat(record.toString())
                .isEqualTo(
                        "MappedRecord (canonicalItemNames=[schema-[key=aKey]], fieldsMap={field1=value1})");
    }

    @Test
    public void shouldCreateMappedRecord() {
        String[] canonicalItemNames =
                List.of(
                                "schema1-[key=aKey]",
                                "schema2",
                                "schema3-[partition=aPartition,value=aValue]")
                        .toArray(new String[0]);

        Map<String, String> fieldsMap = new TreeMap<>();
        fieldsMap.put("field1", "value1");
        fieldsMap.put("field2", "value2");
        fieldsMap.put("field3", null);

        DefaultMappedRecord record = new DefaultMappedRecord(canonicalItemNames, () -> fieldsMap);
        assertThat(record.canonicalItemNames())
                .asList()
                .containsExactly(
                        "schema1-[key=aKey]",
                        "schema2",
                        "schema3-[partition=aPartition,value=aValue]");
        assertThat(record.fieldsMap())
                .containsExactly("field1", "value1", "field2", "value2", "field3", null);
        assertThat(record.toString())
                .isEqualTo(
                        "MappedRecord (canonicalItemNames=[schema1-[key=aKey],schema2,schema3-[partition=aPartition,value=aValue]], fieldsMap={field1=value1, field2=value2, field3=null})");
    }

    @Test
    public void shouldNotHaveCanonicalItemNNamesAndFieldsMapIfNOP() {
        assertThat(DefaultMappedRecord.NOPRecord.canonicalItemNames()).isEmpty();
        assertThat(DefaultMappedRecord.NOPRecord.fieldsMap()).isEmpty();
        assertThat(DefaultMappedRecord.NOPRecord.toString())
                .isEqualTo("MappedRecord (canonicalItemNames=[], fieldsMap={})");
    }

    @Test
    public void shouldRouteParameterizedItems() {
        String canonicalItemName = "schema1-[partition=aPartition,topic=aTopic]";
        String canonicalItemName2 = "schema2-[key=aKey,value=aValue]";
        String[] canonicalItemNames =
                List.of(canonicalItemName, canonicalItemName2).toArray(new String[0]);

        DefaultMappedRecord record = new DefaultMappedRecord(canonicalItemNames);

        // This item should match the expandedTemplate 1: routable
        SubscribedItem matchingItem1 =
                subscribedFrom("schema1-[topic=aTopic,partition=aPartition]");
        // This item should match the expandedTemplate 2: routable
        SubscribedItem matchingItem2 = subscribedFrom("schema2-[key=aKey,value=aValue]");
        // The following items should match no templates: non-routable
        SubscribedItem notMatchingBindParameters =
                subscribedFrom("schema1-[topic=anotherTopic,partition=anotherPartition]");
        SubscribedItem notMatchingSchema = subscribedFrom("schemaX-[key=aKey,value=aValue]");

        SubscribedItems subscribedItems1 = SubscribedItems.create();
        subscribedItems1.addItem(matchingItem1);
        subscribedItems1.addItem(matchingItem2);
        subscribedItems1.addItem(notMatchingBindParameters);
        subscribedItems1.addItem(notMatchingSchema);
        assertThat(record.route(subscribedItems1)).containsExactly(matchingItem1, matchingItem2);

        SubscribedItems subscribedItems2 = SubscribedItems.create();
        subscribedItems2.addItem(notMatchingBindParameters);
        subscribedItems2.addItem(notMatchingSchema);
        assertThat(record.route(subscribedItems2)).isEmpty();
    }

    @Test
    public void shouldRouteSimpleItems() {
        String canonicalItemName1 = "simple-item-1";
        String canonicalItemName2 = "simple-item-2";
        DefaultMappedRecord record =
                new DefaultMappedRecord(
                        List.of(canonicalItemName1, canonicalItemName2).toArray(new String[0]));
        assertThat(record.fieldsMap()).isEmpty();

        SubscribedItem matchingItem1 = subscribedFrom("simple-item-1");
        SubscribedItem matchingItem2 = subscribedFrom("simple-item-2");
        SubscribedItem notMatchingItem = subscribedFrom("simple-item-3");
        SubscribedItems subscribedItems = SubscribedItems.create();
        subscribedItems.addItem(matchingItem1);
        subscribedItems.addItem(matchingItem2);
        subscribedItems.addItem(notMatchingItem);
        assertThat(record.route(subscribedItems)).containsExactly(matchingItem1, matchingItem2);
    }
}
