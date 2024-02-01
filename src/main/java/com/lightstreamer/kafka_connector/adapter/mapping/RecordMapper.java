/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping;

import com.lightstreamer.kafka_connector.adapter.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValuesContainer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface RecordMapper<K, V> {

  interface MappedRecord {

    String topic();

    int mappedValuesSize();

    Map<String, String> filter(Selectors<?, ?> selectors);
  }

  int selectorsSize();

  MappedRecord map(ConsumerRecord<K, V> record);

  static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  static class Builder<K, V> {

    final Set<Selectors<K, V>> allSelectors = new HashSet<>();

    private Builder() {}

    public Builder<K, V> withSelectors(Stream<Selectors<K, V>> selector) {
      allSelectors.addAll(selector.toList());
      return this;
    }

    public final Builder<K, V> withSelectors(Selectors<K, V> selector) {
      allSelectors.add(selector);
      return this;
    }

    public RecordMapper<K, V> build() {
      return new DefaultRecordMapper<>(this);
    }
  }
}

class DefaultRecordMapper<K, V> implements RecordMapper<K, V> {

  protected static Logger log = LoggerFactory.getLogger(DefaultRecordMapper.class);

  private final Set<Selectors<K, V>> selectors;

  DefaultRecordMapper(Builder<K, V> builder) {
    this.selectors = Collections.unmodifiableSet(builder.allSelectors);
  }

  @Override
  public MappedRecord map(ConsumerRecord<K, V> record) {
    Set<ValuesContainer> values =
        selectors.stream().map(s -> s.extractValues(record)).collect(Collectors.toSet());
    return new DefaultMappedRecord(record.topic(), values);
  }

  @Override
  public int selectorsSize() {
    return this.selectors.size();
  }
}

class DefaultMappedRecord implements MappedRecord {

  private final String topic;

  private final Set<ValuesContainer> valuesContainers;

  DefaultMappedRecord(String topic, Set<ValuesContainer> valuesContainers) {
    this.topic = topic;
    this.valuesContainers = valuesContainers;
  }

  @Override
  public String topic() {
    return topic;
  }

  @Override
  public int mappedValuesSize() {
    return valuesContainers.stream().mapToInt(v -> v.values().size()).sum();
  }

  @Override
  public Map<String, String> filter(Selectors<?, ?> selectors) {
    return valuesContainers.stream()
        .filter(container -> container.selectors().equals(selectors))
        .flatMap(container -> container.values().stream())
        .collect(Collectors.toMap(Value::name, Value::text));
  }
}
