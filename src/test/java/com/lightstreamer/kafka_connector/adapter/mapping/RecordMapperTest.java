package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GeneircRecordSelectorsSuppliers;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringSelectorSuppliers;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords;
import com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider;

public class RecordMapperTest {

    @Tag("unit")
    @Test
    public void shouldMap() {
        SelectorsSupplier<String, GenericRecord> selectorsSuppliers = SelectorsSupplier.wrap(
                StringSelectorSuppliers.keySelectorSupplier(),
                GeneircRecordSelectorsSuppliers.valueSelectorSupplier());

        Selectors<String, GenericRecord> nameSelector = Selectors.from(
                selectorsSuppliers,
                Map.of("name", "VALUE.name"));

        Selectors<String, GenericRecord> childSelector1 = Selectors.from(
                selectorsSuppliers,
                Map.of("firstChildName", "VALUE.children[0].name"));

        Selectors<String, GenericRecord> childSelector2 = Selectors.from(
                selectorsSuppliers,
                Map.of("secondChildName", "VALUE.children[1].name",
                        "grandChildName", "VALUE.children[1].children[1].name"));

        RecordMapper<String, GenericRecord> remapper = RecordMapper.<String, GenericRecord>builder()
                .withSelectors(nameSelector)
                .withSelectors(childSelector1)
                .withSelectors(childSelector2)
                .build();

        ConsumerRecord<String, GenericRecord> kafkaRecord = ConsumerRecords.record("", GenericRecordProvider.RECORD);
        MappedRecord remap = remapper.map(kafkaRecord);

        Map<String, String> parentName = remap.filter(nameSelector.schema());
        assertThat(parentName).containsExactly("name", "joe");

        Map<String, String> firstChildName = remap.filter(childSelector1.schema());
        assertThat(firstChildName).containsExactly("firstChildName", "alex");

        Map<String, String> otherPeopleNames = remap.filter(childSelector2.schema());
        assertThat(otherPeopleNames).containsExactly("secondChildName", "anna", "grandChildName", "terence");

        assertThat(remap.filter(Schema.of("nonExistingKey"))).isEmpty();
    }

}
