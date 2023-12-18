package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.DataProviderException;
import com.lightstreamer.interfaces.data.FailureException;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SmartDataProvider;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ListType;
import com.lightstreamer.kafka_connector.adapter.config.ValidateException;
import com.lightstreamer.kafka_connector.adapter.consumers.ConsumerLoop;
import com.lightstreamer.kafka_connector.adapter.consumers.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.consumers.TopicMapping;
import com.lightstreamer.kafka_connector.adapter.mapping.ItemExpressionEvaluator.EvaluationException;
import com.lightstreamer.kafka_connector.adapter.mapping.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.Selectors.SelectorSuppliersPair;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.ValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.avro.GenericRecordValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringKeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.string.StringValueSelectorSupplier;

public class KafkaConnectorAdapter implements SmartDataProvider {

    private final List<TopicMapping> topicMappings = new ArrayList<>();

    private static final ConfigSpec CONFIG_SPEC;

    private Logger log;

    private Map<String, String> configuration;

    private Loop loop;

    private Selectors<?, ?> fieldsSelector;

    private ConsumerLoopConfig<?, ?> loopConfig;

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

    public KafkaConnectorAdapter() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(@Nonnull Map params, @Nonnull File configDir) throws DataProviderException {
        Path path = Paths.get(configDir.getAbsolutePath(), "log4j.properties");
        PropertyConfigurator.configure(path.toString());
        log = LoggerFactory.getLogger(KafkaConnectorAdapter.class);
        try {
            this.configuration = CONFIG_SPEC.parse(params);
            this.configuration.put("adapter.dir", configDir.getAbsolutePath());

            // Retrieve "map.<topic-name>.to"
            for (String paramKey : configuration.keySet()) {
                if (paramKey.startsWith("map.")) {
                    String topic = paramKey.split("\\.")[1];
                    String[] itemTemplates = new String[] { configuration.get(paramKey) };// .split(",");
                    topicMappings.add(new TopicMapping(topic, Arrays.asList(itemTemplates)));
                }
            }

            // Retrieve "field.<name>"
            Map<String, String> fields = new HashMap<>();
            for (String paramKey : configuration.keySet()) {
                if (paramKey.startsWith("field.")) {
                    String fieldName = paramKey.split("\\.")[1];
                    String mapping = configuration.get(paramKey);
                    fields.put(fieldName, mapping);
                }
            }

            String valueConsumer = configuration.get("value.consumer");
            String keyConsumer = configuration.get("key.consumer");

            SelectorSuppliersPair<?, ?> pairSupplier = SelectorSuppliersPair.wrap(makeKeySelectorSupplier(keyConsumer),
                    makeValueSelectorSupplier(valueConsumer));

            fieldsSelector = Selectors.builder(pairSupplier)
                    .withMap(fields)
                    .build();

            ItemTemplates<?, ?> itemTemplates = initItemTemplates(pairSupplier);

            Properties properties = initProps(configuration, pairSupplier);
            loopConfig = ConsumerLoopConfig.of(properties, itemTemplates, fieldsSelector);

            log.info("Init completed");
        } catch (ValidateException ve) {
            throw new DataProviderException(ve.getMessage());
        }
    }

    private Properties initProps(Map<String, String> config, SelectorSuppliersPair<?, ?> pairSupplier) {
        return initConsumerProperties(config, pairSupplier);
    }

    private <K, V> Properties initConsumerProperties(Map<String, String> config,
            SelectorSuppliersPair<K, V> pairSupplier) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap-servers"));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.get("group-id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("adapter.dir", config.get("adapter.dir"));
        Optional.ofNullable(config.get("key.schema.file"))
                .ifPresent(v -> properties.setProperty("key.schema.file", v));
        Optional.ofNullable(config.get("value.schema.file"))
                .ifPresent(v -> properties.setProperty("value.schema.file", v));
        pairSupplier.keySelectorSupplier().configKey(config, properties);
        pairSupplier.valueSelectorSupplier().configValue(config, properties);
        log.info("Properties: {}", properties);
        return properties;
    }

    private ItemTemplates<?, ?> initItemTemplates(SelectorSuppliersPair<?, ?> pairSupplier) throws ValidateException {
        return init(pairSupplier);
    }

    private <K, V> ItemTemplates<K, V> init(SelectorSuppliersPair<K, V> pairSupplier) throws ValidateException {
        try {
            return ItemTemplates.of(topicMappings, pairSupplier);
        } catch (EvaluationException e) {
            throw new ValidateException(e.getMessage());
        }
    }

    @Override
    public boolean isSnapshotAvailable(@Nonnull String itemName) throws SubscriptionException {
        return false;
    }

    @Override
    public void setListener(@Nonnull ItemEventListener eventListener) {
        this.loop = makeLoop(loopConfig, eventListener);
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

    private Loop makeLoop(ConsumerLoopConfig<?, ?> config, ItemEventListener eventListener) {
        return loop(config, eventListener);
    }

    private <K, V> Loop loop(ConsumerLoopConfig<K, V> config, ItemEventListener eventListener) {
        return new ConsumerLoop<>(config, eventListener);
    }

    @Override
    public void subscribe(@Nonnull String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {
    }

    @Override
    public void unsubscribe(@Nonnull String itemName) throws SubscriptionException, FailureException {
        log.info("Unsibscribing from item [{}]", itemName);
        loop.unsubscribe(itemName);
    }

    @Override
    public void subscribe(@Nonnull String itemName, @Nonnull Object itemHandle, boolean needsIterator)
            throws SubscriptionException, FailureException {
        log.info("Trying subscription to item [{}]", itemName);
        loop.trySubscribe(itemName, itemHandle);
    }
}
