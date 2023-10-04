package com.lightstreamer.kafka_connector.adapter.mapping;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SimpleConverter implements DataConverter {

    @Override
    public Map<String, String> convert(byte[] data) {
        // Map<String, String> updates = new HashMap<>();
        // updates.put("key", record.key());
        // updates.put("value", record.value());
        // updates.put("partition", String.valueOf(record.partition()));
        // updates.put("timestamp", String.valueOf(record.timestamp()));
        // updates.put("topic", record.topic());
        // updates.put("offest", String.valueOf(record.offset()));
        // updates.put("headers", record.headers().toString());        
        // TODO Auto-generated method stub
        return Map.of("value", new String(data, StandardCharsets.UTF_8));
    }
    
}
