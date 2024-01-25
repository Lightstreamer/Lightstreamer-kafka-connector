package com.lightstreamer.kafka_connector.adapter.config;

import java.util.Map;

public record InfoItem (Object itemHandle, String fieldName){
 
    public Map<String, String> mkEvent(String value) {
        return Map.of(fieldName(), value);
    }
}
