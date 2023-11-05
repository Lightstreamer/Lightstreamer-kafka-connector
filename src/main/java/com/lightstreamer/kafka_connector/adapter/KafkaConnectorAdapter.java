package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

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
import com.lightstreamer.kafka_connector.adapter.consumers.TopicMapping;
import com.lightstreamer.kafka_connector.adapter.consumers.avro.GenericRecordSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.consumers.json.KeyJsonNodeSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.consumers.json.ValueJsonNodeSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.consumers.raw.RawValueSelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.KeySelectorSupplier;
import com.lightstreamer.kafka_connector.adapter.evaluator.ValueSelectorSupplier;

public class KafkaConnectorAdapter implements SmartDataProvider {

    private final List<TopicMapping> topicMappings = new ArrayList<>();

    private static final ConfigSpec CONFIG_SPEC;

    private Logger log;

    private Map<String, String> configuration;

    private Loop loop;

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
            log.info("Init completed");
        } catch (ValidateException ve) {
            throw new DataProviderException(ve.getMessage());
        }
    }

    @Override
    public boolean isSnapshotAvailable(@Nonnull String itemName) throws SubscriptionException {
        return false;
    }

    @Override
    public void setListener(@Nonnull ItemEventListener eventListener) {
        this.loop = buildLoop(eventListener);
    }

    private Loop buildLoop(ItemEventListener eventListener) {
        String valueConsumer = configuration.get("value.consumer");
        String keyConsumer = configuration.get("key.consumer");

        KeySelectorSupplier<?> k = makeKeySelectorSupplier(keyConsumer);
        ValueSelectorSupplier<?> v = makeValueSelectorSupplier(valueConsumer);

        return makeLoop(k, v, eventListener);
    }

    private KeySelectorSupplier<?> makeKeySelectorSupplier(String consumerType) {
        return switch (consumerType) {
            case "AVRO" -> new GenericRecordSelectorSupplier(configuration, isKey);
            case "JSON" -> new KeyJsonNodeSelectorSupplier();
            case "RAW" -> new RawValueSelectorSupplier(configuration, isKey);
            default -> throw new RuntimeException("No available consumer %s".formatted(consumerType));
        };
    }

    private ValueSelectorSupplier<?> makeValueSelectorSupplier(String consumerType) {
        return switch (consumerType) {
            case "AVRO" -> new GenericRecordSelectorSupplier(configuration, isKey);
            case "JSON" -> new ValueJsonNodeSelectorSupplier();
            case "RAW" -> new RawValueSelectorSupplier(configuration, isKey);
            default -> throw new RuntimeException("No available consumer %s".formatted(consumerType));
        };
    }

    private Loop makeLoop(KeySelectorSupplier<?> k, ValueSelectorSupplier<?> v, ItemEventListener eventListener) {
        return loop(k, v, eventListener);
    }

    private <K, V> Loop loop(KeySelectorSupplier<K> k, ValueSelectorSupplier<V> v, ItemEventListener eventListener) {
        return new ConsumerLoop<>(configuration, topicMappings, k, v, eventListener);
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
