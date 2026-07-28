
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Subscription;

import com.lightstreamer.kafka.adapters.mapping.selectors.others.OthersSelectorSuppliers;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.OnDemandSubscribedItems;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractors;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.ExtractionException;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.test_utils.Mocks;
import com.lightstreamer.kafka.test_utils.Records;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

class MappedRecordTest {

    static Stream<Arguments> provideData() {
        return Stream.of(
                Arguments.of(
                        new String[] {"schema-[key=aKey]"},
                        Map.of(
                                "key",
                                Expressions.Wrapped("#{KEY}"),
                                "value",
                                Expressions.Wrapped("#{VALUE}")),
                        Records.KafkaRecord("aKey", "aValue"),
                        Map.of("key", "aKey", "value", "aValue")),
                Arguments.of(
                        new String[] {"schema-[key=aKey]"},
                        Map.of(
                                "key",
                                Expressions.Wrapped("#{KEY}"),
                                "value",
                                Expressions.Wrapped("#{VALUE}")),
                        Records.KafkaRecord("aKey", null),
                        new HashMap<>() {
                            {
                                put("key", "aKey");
                                put("value", null);
                            }
                        }));
    }

    @ParameterizedTest
    @MethodSource("provideData")
    void shouldCreateMappedRecord(
            String[] canonicalItemNames,
            Map<String, ExtractionExpression> extractionExpressions,
            KafkaRecord<String, String> kafkaRecord,
            Map<String, String> expectedFieldsMap)
            throws ExtractionException {

        FieldsExtractor<String, String> fieldsExtractor =
                DataExtractors.namedFieldsExtractor(
                        OthersSelectorSuppliers.String(), extractionExpressions, false, false);

        MappedRecordImpl record =
                new MappedRecordImpl(
                        canonicalItemNames,
                        new FieldsMapSupplierImpl<>(fieldsExtractor, kafkaRecord));
        assertThat(record.fieldsMap()).containsExactlyEntriesIn(expectedFieldsMap);
        assertThat(record.canonicalItemNames()).isEqualTo(canonicalItemNames);
        assertThat(record.isPayloadNull()).isEqualTo(kafkaRecord.isPayloadNull());

        Set<String> fields = expectedFieldsMap.keySet();
        for (String field : fields) {
            Map<String, String> map = record.fieldsMapFromField(field);
            assertThat(map).hasSize(1);
            assertThat(map).containsEntry(field, expectedFieldsMap.get(field));
        }
    }

    @Test
    void shouldNOPRecordBeEmpty() {
        assertThat(MappedRecordImpl.NOPRecord.canonicalItemNames()).isEmpty();
        assertThat(MappedRecordImpl.NOPRecord.isPayloadNull()).isTrue();
        assertThat(MappedRecordImpl.NOPRecord.fieldsMap()).isEmpty();
        assertThat(MappedRecordImpl.NOPRecord.toString())
                .isEqualTo("MappedRecord(canonicalItemNames=[])");
    }

    @Test
    void shouldRouteParameterizedItems() {
        String canonicalItemName = "schema1-[partition=aPartition,topic=aTopic]";
        String canonicalItemName2 = "schema2-[key=aKey,value=aValue]";
        String[] canonicalItemNames =
                List.of(canonicalItemName, canonicalItemName2).toArray(new String[0]);

        MappedRecordImpl record = new MappedRecordImpl(canonicalItemNames);
        assertThat(record.fieldsMap()).isEmpty();
        assertThat(record.isPayloadNull()).isTrue();

        // This item should match the expandedTemplate 1: routable
        OnDemandSubscribedItem matchingItem1 =
                Items.onDemandSubscribedFrom(
                        Subscription("schema1-[topic=aTopic,partition=aPartition]"), new Object());
        // This item should match the expandedTemplate 2: routable
        OnDemandSubscribedItem matchingItem2 =
                Items.onDemandSubscribedFrom(
                        Subscription("schema2-[key=aKey,value=aValue]"), new Object());
        // The following items should match no templates: non-routable
        OnDemandSubscribedItem notMatchingBindParameters =
                Items.onDemandSubscribedFrom(
                        Subscription("schema1-[topic=anotherTopic,partition=anotherPartition]"),
                        new Object());
        OnDemandSubscribedItem notMatchingSchema =
                Items.onDemandSubscribedFrom(
                        Subscription("schemaX-[key=aKey,value=aValue]"), new Object());

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
    void shouldRouteForcedParameterizedItems() {
        String canonicalItemName = "schema1-[partition=aPartition,topic=aTopic]";
        String canonicalItemName2 = "schema2-[key=aKey,value=aValue]";
        String[] canonicalItemNames =
                List.of(canonicalItemName, canonicalItemName2).toArray(new String[0]);

        MappedRecordImpl record = new MappedRecordImpl(canonicalItemNames);
        assertThat(record.fieldsMap()).isEmpty();
        assertThat(record.isPayloadNull()).isTrue();

        SubscribedItems subscribedItems1 =
                SubscribedItems.forceable(new Mocks.MockItemEventListener(), null);
        Set<SubscribedItem> routed = record.route(subscribedItems1);
        assertThat(routed.stream().map(SubscribedItem::canonicalName))
                .containsExactly(canonicalItemName, canonicalItemName2);
    }

    @Test
    void shouldRouteSimpleItems() {
        String canonicalItemName1 = "simple-item-1";
        String canonicalItemName2 = "simple-item-2";
        MappedRecordImpl record =
                new MappedRecordImpl(
                        List.of(canonicalItemName1, canonicalItemName2).toArray(new String[0]));
        assertThat(record.fieldsMap()).isEmpty();
        assertThat(record.isPayloadNull()).isTrue();

        OnDemandSubscribedItem matchingItem1 =
                Items.onDemandSubscribedFrom(Subscription("simple-item-1"), new Object());
        OnDemandSubscribedItem matchingItem2 =
                Items.onDemandSubscribedFrom(Subscription("simple-item-2"), new Object());
        OnDemandSubscribedItem notMatchingItem =
                Items.onDemandSubscribedFrom(Subscription("simple-item-3"), new Object());
        OnDemandSubscribedItems subscribedItems = SubscribedItems.onDemand();
        subscribedItems.addItem(matchingItem1);
        subscribedItems.addItem(matchingItem2);
        subscribedItems.addItem(notMatchingItem);
        assertThat(record.route(subscribedItems)).containsExactly(matchingItem1, matchingItem2);
    }

    @Test
    void shouldRouteForcedSimpleItems() {
        String canonicalItemName1 = "simple-item-1";
        String canonicalItemName2 = "simple-item-2";
        MappedRecordImpl record =
                new MappedRecordImpl(
                        List.of(canonicalItemName1, canonicalItemName2).toArray(new String[0]));
        assertThat(record.fieldsMap()).isEmpty();
        assertThat(record.isPayloadNull()).isTrue();

        SubscribedItems forcedItems =
                SubscribedItems.forceable(new Mocks.MockItemEventListener(), null);
        Set<SubscribedItem> routed = record.route(forcedItems);

        assertThat(routed.stream().map(SubscribedItem::canonicalName))
                .containsExactly(canonicalItemName1, canonicalItemName2);
    }
}
