package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

public class KafkaConnectorAdapter implements DataProvider {

    private Logger log;

    private Set<String> topics;

    private Map<String, String> configuration;

    private Loop loop;

    private ItemEventListener eventListener;

    static ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC = new ConfigSpec()
                .add("bootstrap-servers", true, new ListType<ConfType>(ConfType.Host))
                .add("group-id", true, ConfType.Text)
                .add("topics", true, new ListType<ConfType>(ConfType.Text))
                .add("item_", true, ConfType.Text);
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
            String topicString = configuration.get("topics");
            this.topics = Arrays.stream(topicString.split(",")).collect(Collectors.toSet());

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

    }

    public void subscribe(String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {

        if (topics.contains(itemName)) {
            this.loop = new GenericRecordConsumerLoop(configuration, eventListener);
            log.info("Subscribed to item [{}]", itemName);
            loop.subscribe(itemName);
        }
    }

    @Override
    public void unsubscribe(String itemName) throws SubscriptionException, FailureException {
        if (topics.contains(itemName)) {
            log.info("Unsubscribed from item [{}]", itemName);
            loop.unsubscribe(itemName);
        }
    }

}
