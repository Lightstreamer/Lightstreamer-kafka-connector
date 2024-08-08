package com.lightstreamer.kafka.examples;

import java.io.File;
import java.util.Map;

import com.lightstreamer.interfaces.metadata.TableInfo;
import com.lightstreamer.interfaces.metadata.CreditsException;
import com.lightstreamer.interfaces.metadata.NotificationException;
import com.lightstreamer.kafka.adapters.pub.KafkaConnectorMetadataAdapter;

public class CustomKafkaConnectorAdapter extends KafkaConnectorMetadataAdapter {

    @Override
    public void postInit(Map parameters, File adapterDir) {
        System.out.println("###### Custom KafkaConnector Adapter initialized ######");
    }

    @Override
    public void onSubscription(String user, String sessionID, TableInfo[] tables) throws CreditsException, NotificationException {
        System.out.format("Subscribed to  from item [%s]%n", tables[0].getId());
    }

    @Override
    public void onUnsubscription(String sessionID, TableInfo[] tables) throws NotificationException {
        System.out.format("Unsubscribed from item [%s]%n", tables[0].getId());
    }

}
