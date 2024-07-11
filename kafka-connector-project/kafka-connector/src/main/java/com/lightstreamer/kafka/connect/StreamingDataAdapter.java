
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

import com.lightstreamer.adapters.remote.DataProvider;
import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.FailureException;
import com.lightstreamer.adapters.remote.ItemEventListener;
import com.lightstreamer.adapters.remote.SubscriptionException;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.mapping.Items;
import com.lightstreamer.kafka.mapping.Items.Item;
import com.lightstreamer.kafka.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.mapping.RecordMapper;
import com.lightstreamer.kafka.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.mapping.selectors.ValueException;
import com.lightstreamer.kafka.mapping.selectors.ValuesExtractor;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamingDataAdapter implements DataProvider {

    private static Logger logger = LoggerFactory.getLogger(StreamingDataAdapter.class);

    private interface DownstreamUpdater {

        void update(Collection<SinkRecord> records);
    }

    private final ValuesExtractor<Object, Object> fieldsExtractor;
    private final ItemTemplates<Object, Object> itemTemplates;
    private final RecordErrorHandlingStrategy errorHandlingStrategy;
    private final ErrantRecordReporter reporter;

    protected final ConcurrentHashMap<String, SubscribedItem> subscribedItems =
            new ConcurrentHashMap<>();
    private volatile ItemEventListener listener;
    private final AtomicInteger itemsCounter = new AtomicInteger(0);

    private final RecordMapper<Object, Object> recordMapper;

    private volatile DownstreamUpdater updater = FAKE_UPDATER;

    private static DownstreamUpdater FAKE_UPDATER =
            records -> {
                logger.info("Skipping record");
            };

    StreamingDataAdapter(
            ItemTemplates<Object, Object> itemTemplates,
            ValuesExtractor<Object, Object> fieldsExtractor,
            SinkTaskContext context,
            RecordErrorHandlingStrategy errorHandlingStrategy) {
        this.itemTemplates = itemTemplates;
        this.fieldsExtractor = fieldsExtractor;
        this.recordMapper =
                RecordMapper.builder()
                        .withExtractor(itemTemplates.extractors())
                        .withExtractor(fieldsExtractor)
                        .build();

        this.errorHandlingStrategy = errorHandlingStrategy;
        this.reporter = errantRecordReporter(context);
    }

    ErrantRecordReporter errantRecordReporter(SinkTaskContext context) {
        try {
            // may be null if DLQ not enabled
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

    @Override
    public void init(Map<String, String> parameters, String configFile)
            throws DataProviderException {
        logger.info("Init parameter from Remote Proxy Adapter: {}", parameters);
    }

    @Override
    public void setListener(ItemEventListener eventListener) {
        this.listener = eventListener;
        logger.info("ItemEventListener set");
    }

    public void streamEvents(Collection<SinkRecord> records) {
        updater.update(records);
    }

    @Override
    public void subscribe(String item) throws SubscriptionException, FailureException {
        logger.info("Trying subscription to item [{}]", item);
        SubscribedItem newItem = Items.subscribedFrom(item);
        try {
            if (!itemTemplates.matches(newItem)) {
                logger.warn("Item [{}] does not match any defined item templates", newItem);
                throw new SubscriptionException("Item does not match any defined item templates");
            }

            logger.info("Subscribed to item [{}]", newItem);
            subscribedItems.put(item, newItem);
            if (itemsCounter.addAndGet(1) == 1) {
                updater = this::update;
            }
        } catch (ExpressionException e) {
            logger.error("", e);
            throw new SubscriptionException(e.getMessage());
        }
    }

    @Override
    public void unsubscribe(String item) throws SubscriptionException, FailureException {
        Item removedItem = subscribedItems.remove(item);
        if (removedItem == null) {
            throw new SubscriptionException(
                    "Unsubscribing from unexpected item [%s]".formatted(item));
        }
        if (itemsCounter.decrementAndGet() == 0) {
            updater = FAKE_UPDATER;
        }
    }

    @Override
    public boolean isSnapshotAvailable(String itemName) throws SubscriptionException {
        return false;
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

    private void handleValueException(SinkRecord sinkRecord, ValueException ve) {
        logger.warn("Error while extracting record: {}", ve.getMessage());
        logger.warn("Applying the {} strategy", errorHandlingStrategy);
        switch (errorHandlingStrategy) {
            case IGNORE_AND_CONTINUE -> {
                logger.warn("Ignoring the error and continuing");
            }
            case FORWARD_TO_DLQ -> {
                if (this.reporter != null) {
                    logger.warn("Forwarding the error to DLQ");
                    reporter.report(sinkRecord, ve);
                } else {
                    logger.warn("Since no DQL has been configured, terminating task");
                    throw new ConnectException(ve);
                }
            }
            case TERMINATE_TASK -> {
                logger.error("Terminating task");
                throw new ConnectException(ve);
            }
        }
    }

    private void updateRecord(SinkRecord sinkRecord) throws ValueException {
        // logger.debug("Mapping incoming Kafka record");
        logger.info("Mapping incoming Kafka record");
        // logger.trace("Kafka record: {}", sinkRecord.toString());
        logger.info("Kafka record: {}", sinkRecord.toString());
        MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(sinkRecord));

        Set<SubscribedItem> routable = itemTemplates.routes(mappedRecord, subscribedItems.values());

        logger.info("Routing record to {} items", routable.size());

        for (SubscribedItem sub : routable) {
            // logger.debug("Filtering updates");
            logger.info("Filtering updates");
            Map<String, String> updates = mappedRecord.filter(fieldsExtractor);
            if (listener != null) {
                // logger.debug("Sending updates: {}", updates);
                logger.info("Sending updates: {}", updates);
                listener.update(sub.itemHandle().toString(), updates, false);
            }
        }
    }
}
