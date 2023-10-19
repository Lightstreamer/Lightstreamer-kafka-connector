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
import com.lightstreamer.kafka_connector.adapter.consumers.GenericRecordConsumerLoop;
import com.lightstreamer.kafka_connector.adapter.consumers.JsonNodeConsumer;
import com.lightstreamer.kafka_connector.adapter.consumers.RawConsumerLoop;
import com.lightstreamer.kafka_connector.adapter.consumers.SymbolConsumerLoop;
import com.lightstreamer.kafka_connector.adapter.consumers.TopicMapping;

public class KafkaConnectorAdapter implements SmartDataProvider {

    private Logger log;

    private Map<String, String> configuration;

    private Loop loop;

    private ItemEventListener eventListener;

    private List<TopicMapping> topicMappings = new ArrayList<>();

    static ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add("bootstrap-servers", true, false, new ListType<ConfType>(ConfType.Host))
                .add("group-id", true, false, ConfType.Text)
                .add("consumer", false, false, ConfType.Text)
                .add("field.", true, true, ConfType.Text)
                .add("map.", true, true, ConfType.Text);
    }

    public KafkaConnectorAdapter() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Map params, File configDir) throws DataProviderException {
        Path path = Paths.get(configDir.getAbsolutePath(), "log4j.properties");
        PropertyConfigurator.configure(path.toString());
        log = LoggerFactory.getLogger(KafkaConnectorAdapter.class);
        try {
            this.configuration = CONFIG_SPEC.parse(params);

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
    public boolean isSnapshotAvailable(String itemName) throws SubscriptionException {
        return false;
    }

    @Override
    public void setListener(ItemEventListener eventListener) {
        this.eventListener = eventListener;
        loop = newLoop(topicMappings);
    }

    private Loop newLoop(List<TopicMapping> mappings) {
        String loopType = configuration.getOrDefault("consumer", "defaultLoop");
        log.info("Creating ConsumerLoop instance <{}>", loopType);
        return switch (loopType) {
            case "SymbolConsumer" -> new SymbolConsumerLoop(configuration, mappings, eventListener);
            case "JsonConsumer" -> new JsonNodeConsumer(configuration, mappings, eventListener);
            case "RawConsumer" -> new RawConsumerLoop(configuration, mappings, eventListener);
            case "defaultLoop" -> new GenericRecordConsumerLoop(configuration, mappings, eventListener);
            default -> throw new RuntimeException("No available consumer " + loopType);
        };
    }

    @Override
    public void subscribe(String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {
    }

    @Override
    public void unsubscribe(String itemName) throws SubscriptionException, FailureException {
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
