package com.lightstreamer.kafka_connector.adapter.consumers.json;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.lightstreamer.kafka_connector.adapter.evaluator.AbstractSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.Value;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.ValueSelectorSupplier;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

public class JsonNodeValueSelectorSupplier extends AbstractSelectorSupplier<JsonNode>
		implements ValueSelectorSupplier<JsonNode> {

	static final class JsonNodeValueSelector extends JsonNodeBaseSelector implements ValueSelector<JsonNode> {

		protected JsonNodeValueSelector(String name, String expression) {
			super(name, expression);
		}

		@Override
		public Value extract(ConsumerRecord<?, JsonNode> record) {
			return super.eval(record.value());
		}
	}

	public JsonNodeValueSelectorSupplier() {
	}

	protected Class<?> getLocalSchemaDeserializer() {
		return JsonLocalSchemaDeserializer.class;
	}

	protected Class<?> getSchemaDeserializer() {
		return KafkaJsonSchemaDeserializer.class;
	}

	@Override
	public void configValue(Map<String, String> conf, Properties props) {
		ValueSelectorSupplier.super.configValue(conf, props);
		// props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, JsonNode.class.getName());
	}

	@Override
	public ValueSelector<JsonNode> selector(String name, String expression) {
		return new JsonNodeValueSelector(name, expression);
	}

	@Override
	public String deserializer(boolean isKey, Properties props) {
		System.out.println("DESERIALIZER");
		return super.deserializer(isKey, props);
	}
}
