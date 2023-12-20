package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.RecordInspector.RemappedRecord;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Value;

public interface RecordInspector<K, V> {

	interface RemappedRecord {

		String topic();

		Map<String, String> filter(Schema schema);
	}

	RemappedRecord extract(ConsumerRecord<K, V> record);

	static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	static class Builder<K, V> {

		final List<Selectors<K, V>> allSelectors = new ArrayList<>();

		private Builder() {
		}

		public Builder<K, V> withItemTemplates(ItemTemplates<K, V> templates) {
			allSelectors.addAll(templates.selectors().toList());
			return this;
		}

		public Builder<K, V> withSelectors(Selectors<K, V> selector) {
			allSelectors.add(selector);
			return this;
		}

		public RecordInspector<K, V> build() {
			return new DefaultRecordInspector<>(this);
		}

	}
}

class DefaultRecordInspector<K, V> implements RecordInspector<K, V> {

	protected static Logger log = LoggerFactory.getLogger(DefaultRecordInspector.class);

	private final List<Selectors<K, V>> selectors;

	DefaultRecordInspector(Builder<K, V> builder) {
		this.selectors = Collections.unmodifiableList(builder.allSelectors);
	}

	@Override
	public RemappedRecord extract(ConsumerRecord<K, V> record) {
		Set<Value> values = selectors.stream().flatMap(s -> s.extract(record).stream()).collect(Collectors.toSet());
		return new DefaultRemappedRecord(record.topic(), values);
	}

}

class DefaultRemappedRecord implements RemappedRecord {

	private final String topic;

	private final Set<Value> valuesSet;

	DefaultRemappedRecord(String topic, Set<Value> values) {
		this.topic = topic;
		this.valuesSet = values;
	}

	@Override
	public String topic() {
		return topic;
	}

	@Override
	public Map<String, String> filter(Schema schema) {
		return valuesSet.stream().filter(v -> schema.keys().contains(v.name()))
				.collect(Collectors.toMap(Value::name, Value::text));
	}
}

// class FakeKeySelectorSupplier<K> implements KeySelectorSupplier<K> {

// @Override
// public KeySelector<K> selector(String name, String expression) {
// throw new UnsupportedOperationException("Unimplemented method 'selector'");
// }
// }

// class FakeValueSelectorSupplier<V> implements ValueSelectorSupplier<V> {

// @Override
// public ValueSelector<V> selector(String name, String expression) {
// throw new UnsupportedOperationException("Unimplemented method 'selector'");
// }

// @Override
// public String deserializer(Properties pros) {
// throw new UnsupportedOperationException("Unimplemented method
// 'deserializer'");
// }

// }

// class FakeKeySelector<K> extends BaseSelector implements KeySelector<K> {

// protected FakeKeySelector(String name, String expression) {
// super(name, expression);
// }

// @Override
// public Value extract(ConsumerRecord<K, ?> record) {
// throw new UnsupportedOperationException("Unimplemented method 'extract'");
// }

// }

// class FakeValueSelector<V> extends BaseSelector implements ValueSelector<V> {

// protected FakeValueSelector(String name, String expression) {
// super(name, expression);
// }

// @Override
// public Value extract(ConsumerRecord<?, V> record) {
// throw new UnsupportedOperationException("Unimplemented method 'extract'");
// }

// }
