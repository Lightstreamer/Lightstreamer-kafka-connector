
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
import com.lightstreamer.kafka_connector.adapters.mapping.ExpressionException;
import com.lightstreamer.kafka_connector.adapters.mapping.Items;
import com.lightstreamer.kafka_connector.adapters.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapters.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper;
import com.lightstreamer.kafka_connector.adapters.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka_connector.adapters.mapping.selectors.Selectors;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StreamingDataAdapter implements DataProvider {

    private static Logger logger = LoggerFactory.getLogger(StreamingDataAdapter.class);

    private volatile ItemEventListener listener;

    private final RecordMapper<Object, Object> recordMapper;

    protected final ConcurrentHashMap<String, Item> subscribedItems = new ConcurrentHashMap<>();

    private final Selectors<Object, Object> fieldsSelectors;

    private final ItemTemplates<Object, Object> itemTemplates;

    StreamingDataAdapter(
            ItemTemplates<Object, Object> itemTemplates,
            Selectors<Object, Object> fieldsSelectors) {
        this.itemTemplates = itemTemplates;
        this.fieldsSelectors = fieldsSelectors;
        this.recordMapper =
                RecordMapper.builder()
                        .withSelectors(itemTemplates.selectors())
                        .withSelectors(fieldsSelectors)
                        .build();
    }

    @Override
    public void setListener(ItemEventListener eventListener) {
        this.listener = listener;
    }

    public void streamEvents(Collection<SinkRecord> records) {
        for (SinkRecord sinkRecord : records) {
            logger.debug("Mapping incoming Kafka record");
            logger.trace("Kafka record: {}", sinkRecord.toString());
            MappedRecord mappedRecord = recordMapper.map(KafkaRecord.from(sinkRecord));

            Set<Item> routable = itemTemplates.routes(mappedRecord, subscribedItems.values());

            logger.info("Routing record to {} items", routable.size());

            for (Item sub : routable) {
                logger.debug("Filtering updates");
                Map<String, String> updates = mappedRecord.filter(fieldsSelectors);
                logger.debug("Sending updates: {}", updates);
                listener.update(sub.itemHandle().toString(), updates, false);
            }
        }
    }

    @Override
    public void subscribe(String item) throws SubscriptionException, FailureException {
        logger.info("Trying subscription to item [{}]", item);
        Item newItem = Items.itemFrom(item);
        try {
            if (itemTemplates.matches(newItem)) {
                logger.warn("Item [{}] does not match any defined item templates", item);
                throw new SubscriptionException("Item does not match any defined item templates");
            }

            logger.info("Subscribed to item [{}]", item);
            subscribedItems.put(item, newItem);
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
    }

    @Override
    public boolean isSnapshotAvailable(String itemName) throws SubscriptionException {
        return false;
    }

    @Override
    public void init(Map<String, String> parameters, String configFile)
            throws DataProviderException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'init'");
    }
}
