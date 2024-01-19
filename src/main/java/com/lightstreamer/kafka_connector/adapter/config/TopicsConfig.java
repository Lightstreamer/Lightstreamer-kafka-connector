package com.lightstreamer.kafka_connector.adapter.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopicsConfig {

    public static record TopicConfiguration(String topic, String itemTemplateKey, String itemTemplateValue) {

    }

    private final List<TopicConfiguration> topicConfigurations;

    private TopicsConfig(ConnectorConfig config) {
        this.topicConfigurations = initConfigurations(config);
    }

    private List<TopicConfiguration> initConfigurations(ConnectorConfig config) {
        List<TopicConfiguration> configs = new ArrayList<>();
        Map<String, String> itemTemplates = config.getValues(ConnectorConfig.ITEM_TEMPLATE, false);
        Map<String, String> topicMappings = config.getValues(ConnectorConfig.TOPIC_MAPPING, true);

        for (Map.Entry<String, String> topicMapping : topicMappings.entrySet()) {
            String topic = topicMapping.getKey();
            String[] itemTemplateRefs = topicMapping.getValue().split(",");
            for (String templateKey : itemTemplateRefs) {
                if (!itemTemplates.containsKey(templateKey)) {
                    throw new ConfigException("No item template [%s] found".formatted(templateKey));
                }
                String templateValue = itemTemplates.get(templateKey);
                configs.add(new TopicConfiguration(topic, templateKey, templateValue));
            }
        }
        return configs;
    }

    public List<TopicConfiguration> configurations() {
        return topicConfigurations;
    }

    public static TopicsConfig of(ConnectorConfig connectorConfig) {
        return new TopicsConfig(connectorConfig);
    }
}
