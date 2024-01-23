package com.lightstreamer.kafka_connector.adapter;

import com.lightstreamer.interfaces.data.SubscriptionException;

public interface Loop {

    void trySubscribe(String item, Object itemHandle) throws SubscriptionException;

    void unsubscribe(String topic);

}