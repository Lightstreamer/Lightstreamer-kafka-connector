package com.lightstreamer.kafka_connector.adapter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.lightstreamer.kafka_connector.adapter.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.config.ConfigException;
import com.lightstreamer.kafka_connector.adapter.consumers.ConsumerLoop;

public class KafkaConnectorAdapter implements SmartDataProvider {

    private Logger log;

    private Loop loop;

    private ConsumerLoopConfig<?, ?> loopConfig;

    public KafkaConnectorAdapter() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(@Nonnull Map params, @Nonnull File configDir) throws DataProviderException {
        Path path = Paths.get(configDir.getAbsolutePath(), "log4j.properties");
        PropertyConfigurator.configure(path.toString());
        log = LoggerFactory.getLogger(KafkaConnectorAdapter.class);
        ConnectorConfigurator configParser = new ConnectorConfigurator(configDir);
        try {
            loopConfig = configParser.configure(params);
            log.info("Init completed");
        } catch (ConfigException ve) {
            throw new DataProviderException(ve.getMessage());
        }
    }

    @Override
    public boolean isSnapshotAvailable(@Nonnull String itemName) throws SubscriptionException {
        return false;
    }

    private <K, V> Loop loop(ConsumerLoopConfig<K, V> config, ItemEventListener eventListener) {
        return new ConsumerLoop<>(config, eventListener);
    }

    private Loop makeLoop(ConsumerLoopConfig<?, ?> config, ItemEventListener eventListener) {
        return loop(config, eventListener);
    }

    @Override
    public void setListener(@Nonnull ItemEventListener eventListener) {
        this.loop = makeLoop(loopConfig, eventListener);
    }

    @Override
    public void subscribe(@Nonnull String itemName, boolean needsIterator)
            throws SubscriptionException, FailureException {
    }

    @Override
    public void subscribe(@Nonnull String itemName, @Nonnull Object itemHandle, boolean needsIterator)
            throws SubscriptionException, FailureException {
        log.info("Trying subscription to item [{}]", itemName);
        loop.trySubscribe(itemName, itemHandle);
    }

    @Override
    public void unsubscribe(@Nonnull String itemName) throws SubscriptionException, FailureException {
        log.info("Unsibscribing from item [{}]", itemName);
        loop.unsubscribe(itemName);
    }
}
