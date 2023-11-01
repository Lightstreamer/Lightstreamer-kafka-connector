package com.lightstreamer.kafka_connector.adapter.evaluator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.lightstreamer.kafka_connector.adapter.consumers.avro.GenericRecordSelector;
import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;

public class ItemTemplateTest {

	static List<? extends Selector<String>> mkList(String... keys) {
		return Arrays.stream(keys).map(key -> new IdentityValueSelector(key, "expr")).toList();
	}

	static ConsumerRecord<String, String> record(String key, String value) {
		return new ConsumerRecord<>("topic", 0, 0, key, value);
	}

	private RecordInspector.Builder<String, String> builder() {
		return new RecordInspector.Builder<>(
				IdentityValueSelector::new, IdentityValueSelector::new);
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, textBlock = """
			INPUT,      EXPECTED
			item,       item
			item-first, item-first
			item_123_,  item_123_
			item-,      item-
			prefix-${}, prefix
			""")
	public void shouldMakeItemWitNoSelectors(String template, String expectedPrefix) {
		ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template, builder());
		assertThat(item).isNotNull();
		assertThat(item.topic()).isEqualTo("topic");
		assertThat(item.prefix()).isEqualTo(expectedPrefix);
		assertThat(item.schemas()).isEmpty();
	}

	@ParameterizedTest
	@EmptySource
	@NullSource
	@ValueSource(strings = { "a,", ".", "|", "@" })
	public void shouldNotMakeItemDueToInvalidTemplate(String invalidTemplate) {
		RuntimeException exception = assertThrows(RuntimeException.class,
				() -> ItemTemplate.makeNew("topic", invalidTemplate, builder()));
		assertThat(exception.getMessage()).isEqualTo("Invalid template");
	}

	@ParameterizedTest
	@NullSource
	public void shouldNotMakeItemDueToInvalidTopic(String topic) {
		RuntimeException exception = assertThrows(
				RuntimeException.class,
				() -> ItemTemplate.makeNew(topic, "template", builder()));
		assertThat(exception.getMessage()).isEqualTo("Invalid topic");
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
			INPUT                             | EXPECTED_NAME |   EXPECTED_SELECTOR
			item-${name=VALUE.field1}         | name          |   VALUE.field1
			item-${name=VALUE.field[0]}       | name          |   VALUE.field[0]
			item-${alias=VALUE.count}         | alias         |   VALUE.count
			item-${alias=VALUE.count.test[*]} | alias         |   VALUE.count.test[*]
			""")
	public void shouldMakeItemWithValueSelector(String template, String expectedName, String expectedSelector) {
		ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template, builder());
		assertThat(item).isNotNull();

		List<Selector<String>> valueSelectors = item.inspector().valueSelectors();
		assertThat(valueSelectors).hasSize(1);

		Selector<String> valueSelector = valueSelectors.get(0);
		assertThat(valueSelector.name()).isEqualTo(expectedName);
		assertThat(valueSelector.expression()).isEqualTo(expectedSelector);
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
			INPUT                             | EXPECTED_NAME |   EXPECTED_SELECTOR
			item-${key=KEY}                   | key           |   KEY
			""")
	public void shouldMakeItemWithKeySelector(String template, String expectedName, String expectedSelector) {
		ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template, builder());
		assertThat(item).isNotNull();

		List<Selector<String>> valueSelectors = item.inspector().keySelectors();
		assertThat(valueSelectors).hasSize(1);

		Selector<String> valueSelector = valueSelectors.get(0);
		assertThat(valueSelector.name()).isEqualTo(expectedName);
		assertThat(valueSelector.expression()).isEqualTo(expectedSelector);
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
			INPUT                                                       | EXPECTED_NAME1 |   EXPECTED_SELECTOR1 |  EXPECTED_NAME2 |  EXPECTED_SELECTOR2
			item-${name1=VALUE.field1,name2=VALUE.field2}               | name1          |   VALUE.field1       |  name2          |  VALUE.field2
			item-${name1=VALUE.field1[0],name2=VALUE.field2.otherField} | name1          |   VALUE.field1[0]    |  name2          |  VALUE.field2.otherField
			""")
	public void shouldMakeItemWithMoreValueSelectors(String template, String expectedName1,
			String expectedSelector1,
			String expectedName2, String expectedSelector2) {
		ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template, builder());
		assertThat(item).isNotNull();

		List<? extends Selector<String>> keySelectors = item.inspector().valueSelectors();
		assertThat(keySelectors).hasSize(2);

		Selector<String> valueSelector1 = keySelectors.get(0);
		assertThat(valueSelector1.name()).isEqualTo(expectedName1);
		assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

		Selector<String> valueSelector2 = keySelectors.get(1);
		assertThat(valueSelector2.name()).isEqualTo(expectedName2);
		assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
			INPUT                                                   | EXPECTED_NAME1 |   EXPECTED_SELECTOR1 |  EXPECTED_NAME2 |  EXPECTED_SELECTOR2
			item-${name1=TIMESTAMP,name2=PARTITION}                 | name1          |   TIMESTAMP          |  name2          |  PARTITION
			item-${name1=TOPIC,name2=PARTITION}                     | name1          |   TOPIC              |  name2          |  PARTITION
			""")
	public void shouldMakeItemWithMoreInfoSelectors(String template, String expectedName1, String expectedSelector1,
			String expectedName2, String expectedSelector2) {
		ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template, builder());
		assertThat(item).isNotNull();

		List<? extends Selector<ConsumerRecord<?, ?>>> infoSelectors = item.inspector().infoSelectors();
		assertThat(infoSelectors).hasSize(2);

		Selector<ConsumerRecord<?, ?>> valueSelector1 = infoSelectors.get(0);
		assertThat(valueSelector1.name()).isEqualTo(expectedName1);
		assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

		Selector<ConsumerRecord<?, ?>> valueSelector2 = infoSelectors.get(1);
		assertThat(valueSelector2.name()).isEqualTo(expectedName2);
		assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
			INPUT                                                   | EXPECTED_NAME1 |   EXPECTED_SELECTOR1 |  EXPECTED_NAME2 |  EXPECTED_SELECTOR2
			item-${name1=KEY.field1,name2=KEY.field2}               | name1          |   KEY.field1         |  name2          |  KEY.field2
			item-${name1=KEY.field1[0],name2=KEY.field2.otherField} | name1          |   KEY.field1[0]      |  name2          |  KEY.field2.otherField
			""")
	public void shouldMakeItemWithMoreKeySelectors(String template, String expectedName1, String expectedSelector1,
			String expectedName2, String expectedSelector2) {
		ItemTemplate<String, String> item = ItemTemplate.makeNew("topic", template, builder());
		assertThat(item).isNotNull();

		List<? extends Selector<String>> keySelectors = item.inspector().keySelectors();
		assertThat(keySelectors).hasSize(2);

		Selector<String> valueSelector1 = keySelectors.get(0);
		assertThat(valueSelector1.name()).isEqualTo(expectedName1);
		assertThat(valueSelector1.expression()).isEqualTo(expectedSelector1);

		Selector<String> valueSelector2 = keySelectors.get(1);
		assertThat(valueSelector2.name()).isEqualTo(expectedName2);
		assertThat(valueSelector2.expression()).isEqualTo(expectedSelector2);
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
			INPUT                       | EXPECTED_NAME |   EXPECTED_SELECTOR
			item-${name1=VALUE.field1}  | name          |   VALUE.field1
			""")
	public void shouldExpandOneValue(String template) {
		ItemTemplate<String, String> itemTemplate = ItemTemplate.makeNew("topic", template, builder());
		Item expanded = itemTemplate.expand(record("key", "record-value"));
		assertThat(expanded.values())
				.containsExactly(
						new SimpleValue("name1", "extracted <record-value>"));
	}

	@ParameterizedTest(name = "[{index}] {arguments}")
	@CsvSource(useHeadersInDisplayName = true, delimiter = '|', textBlock = """
			INPUT                                          | EXPECTED_NAME |   EXPECTED_SELECTOR
			item-${name1=VALUE.field1,name2=VALUE.field2}  | name          |   VALUE.field1
			""")
	public void shouldExpandMoreValues(String template) {
		ItemTemplate<String, String> itemTemplate = ItemTemplate.makeNew("topic", template, builder());
		Item expanded = itemTemplate.expand(record("key", "record-value"));
		assertThat(expanded.values())
				.containsExactly(
						new SimpleValue("name1", "extracted <record-value>"),
						new SimpleValue("name2", "extracted <record-value>"));
	}

	@Test
	public void shouldNotExpand() {
		ItemTemplate<String, String> itemTemplate = ItemTemplate.makeNew("topic", "template", builder());
		Item expanded = itemTemplate.expand(record("key", "record-value"));
		assertThat(expanded.values()).isEmpty();
	}

	@Test
	public void shouldMatchItem() {
		RecordInspector.Builder<String, GenericRecord> builder = new RecordInspector.Builder<>(
				IdentityValueSelector::new,
				GenericRecordSelector::new);
		ItemTemplate<String, GenericRecord> itemTemplate = ItemTemplate
				.makeNew("topic", "kafka-avro-${name=VALUE.User.name,friend=VALUE.User.friends}",
						builder);

		List<Selector<GenericRecord>> valueSelectors = itemTemplate.inspector().valueSelectors();
		assertThat(valueSelectors).hasSize(2);

		Selector<GenericRecord> valueSelector1 = valueSelectors.get(0);
		assertThat(valueSelector1.name()).isEqualTo("name");
		assertThat(valueSelector1.expression()).isEqualTo("VALUE.User.name");

		Selector<GenericRecord> valueSelector2 = valueSelectors.get(1);
		assertThat(valueSelector2.name()).isEqualTo("friend");
		assertThat(valueSelector2.expression()).isEqualTo("VALUE.User.friends");

		Item item = Item.of("kafka-avro-<name=Gianluca>", new Object());
		MatchResult match = itemTemplate.match(item);
		assertThat(match.matched()).isTrue();
	}
}
