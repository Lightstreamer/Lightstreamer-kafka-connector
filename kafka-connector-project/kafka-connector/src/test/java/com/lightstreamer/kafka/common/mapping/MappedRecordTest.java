
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

import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MappedRecordTest {

    static Stream<Arguments> provideRecordsForRouting() {
        return Stream.of(
                Arguments.of(
                        new String[] {"schema-[key=aKey]"},
                        Map.of("field1", "value1"),
                        true,
                        "MappedRecord (canonicalItemNames=[schema-[key=aKey]], fieldsMap={field1=value1})"),
                Arguments.of(
                        new String[] {
                            "schema1-[key=aKey]",
                            "schema2",
                            "schema3-[partition=aPartition,value=aValue]"
                        },
                        new LinkedHashMap<>() {
                            {
                                put("field1", "value1");
                                put("field2", "value2");
                                put("field3", null);
                            }
                        },
                        false,
                        "MappedRecord (canonicalItemNames=[schema1-[key=aKey],schema2,schema3-[partition=aPartition,value=aValue]], fieldsMap={field1=value1, field2=value2, field3=null})"));
    }

    @ParameterizedTest
    @MethodSource("provideRecordsForRouting")
    public void shouldCreateMappedRecord(
            String[] canonicalItemNames,
            Map<String, String> fieldsMap,
            boolean isPayloadNull,
            String expectedToString) {
        MappedRecordImpl record =
                new MappedRecordImpl(canonicalItemNames, () -> fieldsMap, isPayloadNull);
        assertThat(record.fieldsMap()).containsExactlyEntriesIn(fieldsMap);
        assertThat(record.canonicalItemNames()).isEqualTo(canonicalItemNames);
        assertThat(record.isPayloadNull()).isEqualTo(isPayloadNull);
        assertThat(record.toString()).isEqualTo(expectedToString);
    }

    @Test
    public void shouldNOPRecordBeEmpty() {
        assertThat(MappedRecordImpl.NOPRecord.canonicalItemNames()).isEmpty();
        assertThat(MappedRecordImpl.NOPRecord.isPayloadNull()).isTrue();
        assertThat(MappedRecordImpl.NOPRecord.fieldsMap()).isEmpty();
        assertThat(MappedRecordImpl.NOPRecord.toString())
                .isEqualTo("MappedRecord (canonicalItemNames=[], fieldsMap={})");
    }

    @Test
    public void shouldRouteParameterizedItems() {
        String canonicalItemName = "schema1-[partition=aPartition,topic=aTopic]";
        String canonicalItemName2 = "schema2-[key=aKey,value=aValue]";
        String[] canonicalItemNames =
                List.of(canonicalItemName, canonicalItemName2).toArray(new String[0]);

        MappedRecordImpl record = new MappedRecordImpl(canonicalItemNames);
        assertThat(record.fieldsMap()).isEmpty();
        assertThat(record.isPayloadNull()).isTrue();

        // This item should match the expandedTemplate 1: routable
        OnDemandSubscribedItem matchingItem1 =
                Items.onDemandSubscribedItem(
                        "schema1-[topic=aTopic,partition=aPartition]", new Object());
        // This item should match the expandedTemplate 2: routable
        OnDemandSubscribedItem matchingItem2 =
                Items.onDemandSubscribedItem("schema2-[key=aKey,value=aValue]", new Object());
        // The following items should match no templates: non-routable
        OnDemandSubscribedItem notMatchingBindParameters =
                Items.onDemandSubscribedItem(
                        "schema1-[topic=anotherTopic,partition=anotherPartition]", new Object());
        OnDemandSubscribedItem notMatchingSchema =
                Items.onDemandSubscribedItem("schemaX-[key=aKey,value=aValue]", new Object());

        OnDemandSubscribedItems subscribedItems1 = SubscribedItems.onDemand();
        subscribedItems1.addItem(matchingItem1);
        subscribedItems1.addItem(matchingItem2);
        subscribedItems1.addItem(notMatchingBindParameters);
        subscribedItems1.addItem(notMatchingSchema);
        assertThat(record.route(subscribedItems1)).containsExactly(matchingItem1, matchingItem2);

        OnDemandSubscribedItems subscribedItems2 = SubscribedItems.onDemand();
        subscribedItems2.addItem(notMatchingBindParameters);
        subscribedItems2.addItem(notMatchingSchema);
        assertThat(record.route(subscribedItems2)).isEmpty();
    }

    @Test
    public void shouldRouteSimpleItems() {
        String canonicalItemName1 = "simple-item-1";
        String canonicalItemName2 = "simple-item-2";
        MappedRecordImpl record =
                new MappedRecordImpl(
                        List.of(canonicalItemName1, canonicalItemName2).toArray(new String[0]));
        assertThat(record.fieldsMap()).isEmpty();
        assertThat(record.isPayloadNull()).isTrue();

        OnDemandSubscribedItem matchingItem1 =
                Items.onDemandSubscribedItem("simple-item-1", new Object());
        OnDemandSubscribedItem matchingItem2 =
                Items.onDemandSubscribedItem("simple-item-2", new Object());
        OnDemandSubscribedItem notMatchingItem =
                Items.onDemandSubscribedItem("simple-item-3", new Object());
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        subscribedItems.addItem(matchingItem1);
        subscribedItems.addItem(matchingItem2);
        subscribedItems.addItem(notMatchingItem);
        assertThat(record.route(subscribedItems)).containsExactly(matchingItem1, matchingItem2);
    }
}
