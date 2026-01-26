
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.connect;

import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.FailureException;
import com.lightstreamer.adapters.remote.ItemEventListener;
import com.lightstreamer.adapters.remote.SubscriptionException;
import com.lightstreamer.kafka.common.listeners.EventListener;
import com.lightstreamer.kafka.common.mapping.Items;
import com.lightstreamer.kafka.common.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.connect.common.RecordSender;
import com.lightstreamer.kafka.connect.config.DataAdapterConfig;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public final class StreamingDataAdapter implements RecordSender {

    /** An interface used internally to forward records to a downstream destination. */
    interface DownstreamUpdater {

        /**
         * Forwards the records to the downstream destination.
         *
         * <p>The method is invoked by the StreamingDataAdapter for each collection of SinkRecord
         * provided by Kafka Connect.
         *
         * @param records the collection of records to forward
         */
        void update(Collection<SinkRecord> records);
    }

    private static Logger logger = LoggerFactory.getLogger(StreamingDataAdapter.class);

    static final DownstreamUpdater NOP_UPDATER =
            records -> {
                logger.debug("Skipping record");
            };

    // The ItemTemplate instance for enabling the Filtered Routing.
    private final ItemTemplates<Object, Object> itemTemplates;

    // The RecordErrorHandlingStrategy instance for managing records that cannot be processed
    // successfully.
    private final RecordErrorHandlingStrategy errorHandlingStrategy;

    // The ErrantRecordReporter instance for forwarding unprocessable records to the DQL.
    private final ErrantRecordReporter reporter;

    /** A container of all currently subscribed items. */
    private final SubscribedItems subscribed = SubscribedItems.create();

    // The EventListener instance injected by the Remote Provider Server.
    private volatile EventListener listener;

    // The lock used to synchronize the replacement of the DownstreamUpdater instance.
    private final ReentrantLock updaterLock = new ReentrantLock();

    // The counter of all subscribed items.
    private final AtomicInteger itemsCounter = new AtomicInteger(0);

    // The RecordMapper instance configured to map the SinkRecord to a flat
    private final RecordMapper<Object, Object> recordMapper;

    // The current DownstreamUpdater for managing incoming records.
    private volatile DownstreamUpdater updater = NOP_UPDATER;

    // The offsets map.
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    // The initialization parameters received from Proxy Adapter.
    private Map<String, String> initParameters = Collections.emptyMap();

    StreamingDataAdapter(DataAdapterConfig config, DownstreamUpdater nopUpdater) {
        this.itemTemplates = config.itemTemplates();
        this.recordMapper =
                RecordMapper.builder()
                        .withCanonicalItemExtractors(itemTemplates.groupExtractors())
                        .enableRegex(itemTemplates.isRegexEnabled())
                        .withFieldExtractor(config.fieldsExtractor())
                        .build();

        this.errorHandlingStrategy = config.recordErrorHandlingStrategy();
        this.reporter = errantRecordReporter(config.context());
        this.updater = nopUpdater;
    }

    StreamingDataAdapter(DataAdapterConfig config) {
        this(config, NOP_UPDATER);
    }

    @Override
    public void init(Map<String, String> parameters, String configFile)
            throws DataProviderException {
        logger.info("Init parameter from Remote Proxy Adapter: {}", parameters);
        this.initParameters = Collections.unmodifiableMap(parameters);
    }

    @Override
    public void setListener(ItemEventListener eventListener) {
        // The listener is set before any subscribe is called and never changes.
        this.listener = EventListener.remoteEventListener(eventListener);
        logger.info("ItemEventListener set");
    }

    @Override
    public void sendRecords(Collection<SinkRecord> records) {
        updater.update(records);
    }

    @Override
    public void subscribe(String item) throws SubscriptionException, FailureException {
        logger.info("Trying subscription to item [{}]", item);
        try {
            SubscribedItem newItem = Items.subscribedFrom(item);
            if (!itemTemplates.matches(newItem)) {
                logger.warn("Item [{}] does not match any defined item templates", newItem);
                throw new SubscriptionException("Item does not match any defined item templates");
            }

            logger.info("Subscribed to item [{}]", item);
            subscribed.addItem(newItem);
            if (itemsCounter.addAndGet(1) == 1) {
                setDownstreamUpdater(this::update);
            }
            newItem.enableRealtimeEvents(listener);
        } catch (ExpressionException e) {
            logger.error("", e);
            throw new SubscriptionException(e.getMessage());
        }
    }

    @Override
    public void unsubscribe(String item) throws SubscriptionException, FailureException {
        Optional<SubscribedItem> removedItem = subscribed.removeItem(item);
        if (removedItem.isEmpty()) {
            throw new SubscriptionException(
                    "Unsubscribing from unexpected item [%s]".formatted(item));
        }
        if (itemsCounter.decrementAndGet() == 0) {
            setDownstreamUpdater(NOP_UPDATER);
        }
    }

    @Override
    public boolean isSnapshotAvailable(String itemName) throws SubscriptionException {
        return false;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("PreCommit phase, current offset: {}", currentOffsets);
        return currentOffsets;
    }

    ErrantRecordReporter errantRecordReporter(SinkTaskContext context) {
        try {
            // May be null if DLQ is not enabled.
            ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
            if (errantRecordReporter != null) {
                logger.info("Errant record reporter not configured.");
            }
            return errantRecordReporter;
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            logger.warn(
                    "Apache Kafka versions prior to 2.6 do not support the errant record reporter.");
            return null;
        }
    }

    // Only for testing purposes
    ErrantRecordReporter getErrantRecordReporter() {
        return reporter;
    }

    // Only for testing purposes
    SubscribedItem getSubscribedItem(String item) {
        return subscribed.getItem(item);
    }

    // Only for testing purposes
    int getCurrentItemsCount() {
        return itemsCounter.get();
    }

    // Only for testing purposes
    DownstreamUpdater getUpdater() {
        return updater;
    }

    // Only for testing purposes
    EventListener getEventListener() {
        return listener;
    }

    // Only for testing purposes
    Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    // Only for testing purposes
    Map<String, String> getInitParameters() {
        return initParameters;
    }

    void saveOffsets(SinkRecord record) {
        currentOffsets.put(
                new TopicPartition(record.originalTopic(), record.originalKafkaPartition()),
                new OffsetAndMetadata(record.originalKafkaOffset() + 1, null));
    }

    private void setDownstreamUpdater(DownstreamUpdater updater) {
        updaterLock.lock();
        try {
            this.updater = updater;
        } finally {
            updaterLock.unlock();
        }
    }

    private void update(Collection<SinkRecord> records) {
        for (SinkRecord sinkRecord : records) {
            try {
                updateRecord(sinkRecord);
            } catch (ValueException ve) {
                handleValueException(sinkRecord, ve);
            }
        }
    }

    private void handleValueException(SinkRecord record, ValueException ve) {
        logger.warn("Error while extracting record: {}", ve.getMessage());
        logger.warn("Applying the {} strategy", errorHandlingStrategy);
        switch (errorHandlingStrategy) {
            case IGNORE_AND_CONTINUE -> {
                logger.warn("Ignoring the error and continuing");
                saveOffsets(record);
            }
            case FORWARD_TO_DLQ -> {
                if (this.reporter != null) {
                    logger.warn("Forwarding the error to DLQ");
                    reporter.report(record, ve);
                    saveOffsets(record);
                } else {
                    logger.warn("Since no DQL has been configured, terminating task");
                    throw new ConnectException("No DQL, terminating task", ve);
                }
            }
            case TERMINATE_TASK -> {
                logger.error("Terminating task");
                throw new ConnectException("Terminating task", ve);
            }
        }
    }

    private void updateRecord(SinkRecord record) throws ValueException {
        logger.debug("Mapping incoming Kafka record");
        logger.trace("Kafka record: {}", record.toString());
        MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(record));
        logger.debug("Mapped Kafka record");

        Set<SubscribedItem> routable = mappedRecord.route(subscribed);

        logger.debug("Filtering updates");
        Map<String, String> updates = mappedRecord.fieldsMap();

        logger.info("Routing record to {} items", routable.size());
        for (SubscribedItem sub : routable) {
            logger.debug("Sending updates: {}", updates);
            // listener.update(sub.itemHandle().toString(), updates, false);
            sub.sendRealtimeEvent(updates, listener);
        }

        saveOffsets(record);
    }
}
