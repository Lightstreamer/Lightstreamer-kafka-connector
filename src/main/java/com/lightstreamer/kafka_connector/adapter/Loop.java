package com.lightstreamer.kafka_connector.adapter;

import java.util.Optional;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;

public interface Loop {

    Item subscribe(String item, Object itemHandle) throws SubscriptionException;

    Item unsubscribe(String topic) throws SubscriptionException;

}