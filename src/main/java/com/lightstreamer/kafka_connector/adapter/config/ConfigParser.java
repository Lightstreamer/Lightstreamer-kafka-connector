package com.lightstreamer.kafka_connector.adapter.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.config.ConfigParser.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ListType;
import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.mapping.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.TopicMapping;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors.SelectorsSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringValueSelectorSupplier;

public class ConfigParser {

    public interface ConsumerLoopConfig<K, V> {

        Properties consumerProperties();

        Selectors<K, V> fieldsSelectors();

        ItemTemplates<K, V> itemTemplates();
    }

    private static final ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add("bootstrap-servers", true, false, new ListType<ConfType>(ConfType.Host))
                .add("group-id", true, false, ConfType.Text)
                .add("consumer", false, false, ConfType.Text)
                .add("value.schema.file", false, false, ConfType.Text)
                .add("value.consumer", true, false, ConfType.Text)
                .add("key.schema.file", false, false, ConfType.Text)
                .add("key.consumer", false, false, ConfType.Text)
                .add("field.", true, true, ConfType.Text)
                .add("map.", true, true, ConfType.Text);
    }

    private Logger log = LoggerFactory.getLogger(ConfigParser.class);

    public ConfigParser() {
    }

    private ItemTemplates<?, ?> initItemTemplates(List<TopicMapping> topicMappings,
            SelectorsSupplier<?, ?> selectorsSupplier)
            throws ValidateException {
        return initItemTemplatesHelper(topicMappings, selectorsSupplier);
    }

    private <K, V> ItemTemplates<K, V> initItemTemplatesHelper(List<TopicMapping> topicMappings,
            SelectorsSupplier<K, V> selectorsSupplier)
            throws ValidateException {
        try {
            return ItemTemplates.of(topicMappings, selectorsSupplier);
        } catch (EvaluationException e) {
            throw new ValidateException(e.getMessage());
        }
    }

    private Properties initProperties(Map<String, String> config, SelectorsSupplier<?, ?> selectorsSupplier) {
        return initPropertiesHelper(config, selectorsSupplier);
    }

    private <K, V> Properties initPropertiesHelper(Map<String, String> config,
            SelectorsSupplier<K, V> selectorsSupplier) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap-servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.get("group-id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("adapter.dir", config.get("adapter.dir"));
        Optional.ofNullable(config.get("key.schema.file"))
                .ifPresent(v -> properties.setProperty("key.schema.file", v));
        Optional.ofNullable(config.get("value.schema.file"))
                .ifPresent(v -> properties.setProperty("value.schema.file", v));
        selectorsSupplier.keySelectorSupplier().configKey(config, properties);
        selectorsSupplier.valueSelectorSupplier().configValue(config, properties);
        return properties;
    }

    <K, V> ConsumerLoopConfig<K, V> loopConfig(Properties props, ItemTemplates<K, V> it, Selectors<K, V> f) {
        return new DefaultConsumerLoopConfig<>(props, it, f);
    }

    private KeySelectorSupplier<?> makeKeySelectorSupplier(String consumerType) {
        return switch (consumerType) {
            case "AVRO" -> new GenericRecordKeySelectorSupplier();
            case "JSON" -> new JsonNodeKeySelectorSupplier();
            case "RAW" -> new StringKeySelectorSupplier();
            default -> throw new RuntimeException("No available consumer %s".formatted(consumerType));
        };
    }

    private ValueSelectorSupplier<?> makeValueSelectorSupplier(String consumerType) {
        return switch (consumerType) {
            case "AVRO" -> new GenericRecordValueSelectorSupplier();
            case "JSON" -> new JsonNodeValueSelectorSupplier();
            case "RAW" -> new StringValueSelectorSupplier();
            default -> throw new RuntimeException("No available consumer %s".formatted(consumerType));
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ConsumerLoopConfig<?, ?> parse(Map<String, String> params) throws ValidateException {
        Map<String, String> configuration = CONFIG_SPEC.parse(params);

        // Retrieve "map.<topic-name>.to"
        List<TopicMapping> topicMappings = new ArrayList<>();
        for (String paramKey : configuration.keySet()) {
            if (paramKey.startsWith("map.")) {
                String topic = paramKey.split("\\.")[1];
                String[] itemTemplates = new String[] { configuration.get(paramKey) };// .split(",");
                topicMappings.add(new TopicMapping(topic, Arrays.asList(itemTemplates)));
            }
        }

        // Retrieve "field.<name>"
        Map<String, String> fieldsMapping = new HashMap<>();
        for (String paramKey : configuration.keySet()) {
            if (paramKey.startsWith("field.")) {
                String fieldName = paramKey.split("\\.")[1];
                String mappingExpression = configuration.get(paramKey);
                fieldsMapping.put(fieldName, mappingExpression);
            }
        }

        SelectorsSupplier<?, ?> selectorsSupplier = SelectorsSupplier.wrap(
                makeKeySelectorSupplier(configuration.get("key.consumer")),
                makeValueSelectorSupplier(configuration.get("value.consumer")));

        Properties props = initProperties(configuration, selectorsSupplier);
        ItemTemplates<?, ?> itemTemplates = initItemTemplates(topicMappings, selectorsSupplier);
        Selectors<?, ?> fieldsSelectors = Selectors.builder(selectorsSupplier)
                .withMap(fieldsMapping)
                .build();

        return new DefaultConsumerLoopConfig(props, itemTemplates, fieldsSelectors);
    }

}

record DefaultConsumerLoopConfig<K, V>(
        Properties consumerProperties, ItemTemplates<K, V> itemTemplates,
        Selectors<K, V> fieldsSelectors) implements ConsumerLoopConfig<K, V> {
}
