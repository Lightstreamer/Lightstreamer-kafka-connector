/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.Builder;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConnectorConfigProvider;
import com.lightstreamer.kafka_connector.adapter.test_utils.ConsumerRecords;
import com.lightstreamer.kafka_connector.adapter.test_utils.GenericRecordProvider;
import com.lightstreamer.kafka_connector.adapter.test_utils.SelectorsSuppliers;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class RecordMapperAvroTest {

  private static Selectors<String, GenericRecord> selectors(
      String schemaName, Map<String, String> entries) {
    return Selectors.from(
        SelectorsSuppliers.avroValue(
            ConnectorConfigProvider.minimalWith(
                Map.of(
                    ConnectorConfig.ADAPTER_DIR,
                    "src/test/resources",
                    ConnectorConfig.VALUE_SCHEMA_FILE,
                    "value.avsc"))),
        schemaName,
        entries);
  }

  private static Builder<String, GenericRecord> builder() {
    return RecordMapper.<String, GenericRecord>builder();
  }

  @Tag("unit")
  @Test
  public void shouldBuildEmptyMapper() {
    RecordMapper<String, GenericRecord> mapper = builder().build();
    assertThat(mapper).isNotNull();
    assertThat(mapper.selectorsSize()).isEqualTo(0);
  }

  @Tag("unit")
  @Test
  public void shouldBuildMapperWithDuplicateSelectors() {
    RecordMapper<String, GenericRecord> mapper =
        builder()
            .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
            .withSelectors(selectors("test", Map.of("aKey", "PARTITION")))
            .build();

    assertThat(mapper).isNotNull();
    assertThat(mapper.selectorsSize()).isEqualTo(1);
  }

  @Tag("unit")
  @Test
  public void shouldBuildMapperWithDifferentSelectors() {
    RecordMapper<String, GenericRecord> mapper =
        builder()
            .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
            .withSelectors(selectors("test2", Map.of("aKey", "PARTITION")))
            .build();

    assertThat(mapper).isNotNull();
    assertThat(mapper.selectorsSize()).isEqualTo(2);
  }

  @Tag("integration")
  @Test
  public void shoulMapEmpty() {
    RecordMapper<String, GenericRecord> mapper = builder().build();

    ConsumerRecord<String, GenericRecord> kafkaRecord =
        ConsumerRecords.record("", GenericRecordProvider.RECORD);
    MappedRecord mappedRecord = mapper.map(kafkaRecord);

    // No expected values because no selectors have been bound to the RecordMapper.
    assertThat(mappedRecord.mappedValuesSize()).isEqualTo(0);
  }

  @Tag("integration")
  @Test
  public void shoulMapWithValues() {
    RecordMapper<String, GenericRecord> mapper =
        builder()
            .withSelectors(selectors("test1", Map.of("aKey", "PARTITION")))
            .withSelectors(selectors("test2", Map.of("aKey", "TOPIC")))
            .withSelectors(selectors("test3", Map.of("aKey", "TIMESTAMP")))
            .build();

    ConsumerRecord<String, GenericRecord> kafkaRecord =
        ConsumerRecords.record("", GenericRecordProvider.RECORD);
    MappedRecord mappedRecord = mapper.map(kafkaRecord);
    assertThat(mappedRecord.mappedValuesSize()).isEqualTo(3);
  }

  @Tag("integration")
  @Test
  public void shoulNotFilterDueToUnboundSelectors() {
    RecordMapper<String, GenericRecord> mapper =
        builder().withSelectors(selectors("test", Map.of("name", "PARTITION"))).build();

    ConsumerRecord<String, GenericRecord> kafkaRecord =
        ConsumerRecords.record("", GenericRecordProvider.RECORD);
    MappedRecord mappedRecord = mapper.map(kafkaRecord);

    assertThat(mappedRecord.mappedValuesSize()).isEqualTo(1);
    Selectors<String, GenericRecord> unbonudSelectors =
        selectors("test", Map.of("name", "VALUE.any"));
    assertThat(mappedRecord.filter(unbonudSelectors)).isEmpty();
  }

  @Tag("integration")
  @Test
  public void shouldFilter() {
    Selectors<String, GenericRecord> nameSelectors =
        selectors("test", Map.of("name", "VALUE.name"));

    Selectors<String, GenericRecord> childSelectors1 =
        selectors("test", Map.of("firstChildName", "VALUE.children[0].name"));

    Selectors<String, GenericRecord> childSelectors2 =
        selectors(
            "test",
            Map.of(
                "secondChildName",
                "VALUE.children[1].name",
                "grandChildName",
                "VALUE.children[1].children[1].name"));

    RecordMapper<String, GenericRecord> mapper =
        builder()
            .withSelectors(nameSelectors)
            .withSelectors(childSelectors1)
            .withSelectors(childSelectors2)
            .build();

    ConsumerRecord<String, GenericRecord> kafkaRecord =
        ConsumerRecords.record("", GenericRecordProvider.RECORD);
    MappedRecord mappedRecord = mapper.map(kafkaRecord);
    assertThat(mappedRecord.mappedValuesSize()).isEqualTo(4);

    Map<String, String> parentName = mappedRecord.filter(nameSelectors);
    assertThat(parentName).containsExactly("name", "joe");

    Map<String, String> firstChildName = mappedRecord.filter(childSelectors1);
    assertThat(firstChildName).containsExactly("firstChildName", "alex");

    Map<String, String> otherPeopleNames = mappedRecord.filter(childSelectors2);
    assertThat(otherPeopleNames)
        .containsExactly("secondChildName", "anna", "grandChildName", "terence");
  }
}
