package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.google.common.truth.BooleanSubject;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordInspector.RemappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider;

public class ItemTemplatesTest {

    // @Tag("unit")
    // @ParameterizedTest
    // @EmptySource
    // @ValueSource(strings = { "a,", ".", "|", "@" })
    // public void shouldNotCreateDueToInvalidTemplate(String invalidTemplate) {
    // RuntimeException exception = assertThrows(RuntimeException.class,
    // () -> ItemTemplate.of("topic", invalidTemplate,
    // RecordInspector.builder()));
    // assertThat(exception.getMessage()).isEqualTo("Invalid item");
    // }

    @Tag("integration")
    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
                            TEMPLATE                                                                     | SUBCRIBING_ITEM                        | MATCH
                            kafka-avro-${keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<name=joe>                  | true
                            kafka-avro-${keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<name=joe,child=alex>       | true
                            kafka-avro-${keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<child=alex>                | true
                            kafka-avro-${keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro                             | true
                            kafka-avro-${keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-                            | false
                            kafka-avro-${keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<keyName=joe>               | true
                            kafka-avro-${keyName=KEY.name,name=VALUE.name,child=VALUE.children[0].name}  | kafka-avro-<keyName=joe,child=alex>    | true
                            kafka-avro-${keyName=KEY.name,child=VALUE.children[1].children[0].name}      | kafka-avro-<keyName=joe,child=gloria>  | true
                            kafka-avro-${keyName=KEY.name,child=VALUE.children[1].children[1].name}      | kafka-avro-<keyName=joe,child=terence> | true
                            kafka-avro-${keyName=KEY.name,child=VALUE.children[1].children[1].name}      | kafka-avro-<keyName=joe,child=carol>   | false
                            kafka-avro-${child=VALUE.children[1].children[1].name}                       | kafka-avro-<keyName=joe,child=terence> | false
                            kafka-avro-${child=VALUE.children[1].children[1].name}                       | kafka-avro-<child=terence>             | true
            #               kafka-avro-${child=VALUE.children[1].children[2].name}                       | kafka-avro-<child=terence>             | true
                            item-${ts=TIMESTAMP,partition=PARTITION}                                     | item-<ts=-1>                           | true
                            item-${ts=TIMESTAMP,partition=PARTITION}                                     | item-<partition=150>                   | true
                            item-${ts=TIMESTAMP,partition=PARTITION}                                     | item-<partition=150,ts=-1>             | true
                            item-${ts=TIMESTAMP,partition=PARTITION}                                     | item-<partition=150,ts=1>              | false
                            item-${ts=TIMESTAMP,partition=PARTITION}                                     | item-<partition=50>                    | false
                            item                                                                         | item                                   | true
                            item-first                                                                   | item-first                             | true
                            item_123_                                                                    | item_123_                              | true
                            item-                                                                        | item-                                  | true
                            prefix-${}                                                                   | prefix                                 | true
                            prefix-${}                                                                   | prefix-                                | false
            #        x       kafka-avro-${KEY}
                                """)
    public void shouldExpand(String template, String subscribingItem, boolean matched) {
        SelectorsSupplier<GenericRecord, GenericRecord> selectionsSupplier = SelectorsSupplier.genericRecord();

        List<TopicMapping> tp = new ArrayList<>();
        tp.add(new TopicMapping("topic", List.of(template)));
        ItemTemplates<GenericRecord, GenericRecord> itemTemplates = Items.templatesFrom(tp, selectionsSupplier);
        RecordInspector<GenericRecord, GenericRecord> inspector = RecordInspector
                .<GenericRecord, GenericRecord>builder()
                .withItemTemplates(itemTemplates)
                .build();

        ConsumerRecord<GenericRecord, GenericRecord> incomingRecord = record("topic",
                GenericRecordProvider.RECORD, GenericRecordProvider.RECORD);
        RemappedRecord remappedRecord = inspector.extract(incomingRecord);
        Item subscribedItem = Items.itemFrom(subscribingItem, new Object());
        Stream<Item> expandedItem = itemTemplates.expand(remappedRecord);
        Optional<Item> first = expandedItem.findFirst();

        assertThat(first.isPresent()).isTrue();

        Item it = first.get();
        boolean match = it.matches(subscribedItem);
        BooleanSubject assertion = assertThat(match);
        if (matched) {
            assertion.isTrue();
        } else {
            assertion.isFalse();
        }
        ;
    }
}

