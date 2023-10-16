package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.interfaces.data.DataProvider;
import com.lightstreamer.interfaces.data.DataProviderException;
import com.lightstreamer.interfaces.data.FailureException;
import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.ListType;
import com.lightstreamer.kafka_connector.adapter.config.ValidateException;
import com.lightstreamer.kafka_connector.adapter.consumers.AbstractConsumerLoop.TopicMapping;
import com.lightstreamer.kafka_connector.adapter.consumers.GenericRecordConsumerLoop;
import com.lightstreamer.kafka_connector.adapter.consumers.JsonNodeConsumer;
import com.lightstreamer.kafka_connector.adapter.consumers.SymbolConsumerLoop;

public class KafkaConnectorAdapter implements DataProvider {

    private Logger log;

    private Map<String, String> configuration;

    private List<Loop> loops = new ArrayList<>();

    private ItemEventListener eventListener;

    private Set<TopicMapping> topicMappings = new HashSet<>();

    static ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add("bootstrap-servers", true, false, new ListType<ConfType>(ConfType.Host))
                .add("group-id", true, false, ConfType.Text)
                .add("consumer", false, false, ConfType.Text)
                .add("item_", true, true, ConfType.Text)
                .add("map.", true, true, ConfType.ItemSpec);
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
                    String[] itemTemplates = new String[]{configuration.get(paramKey)};//.split(",");
                    topicMappings.add(new TopicMapping(topic, itemTemplates));
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
        for (TopicMapping topicMapping : topicMappings) {
            loops.add(newLoop(topicMapping));
        }
    }

    Loop newLoop(TopicMapping mapping) {
        String loopType = configuration.getOrDefault("consumer", "defaultLoop");
        log.info("Creating a <{}> ConsumerLoop instance for topic <{}>", loopType, mapping.topic());
        return switch (loopType) {
            case "SymbolConsumer" -> new SymbolConsumerLoop(configuration, mapping, eventListener);
            case "JsonConsumer" -> new JsonNodeConsumer(configuration, mapping, eventListener);
            case "defaultLoop" -> new GenericRecordConsumerLoop(configuration, mapping, eventListener);
            default -> throw new RuntimeException("No available consumer " + loopType);
        };
    }

    public void subscribe(String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {

        log.info("Subscribing to item [{}]", itemName);
        loops.stream()
                .filter(l -> l.maySubscribe(itemName))
                .forEach(l -> {
                    l.subscribe(itemName);
                });
    }

    @Override
    public void unsubscribe(String itemName) throws SubscriptionException, FailureException {
        // if (topics.contains(itemName)) {
        // log.info("Unsubscribed from item [{}]", itemName);
        // // loops.unsubscribe(itemName);
        // }
    }

}
