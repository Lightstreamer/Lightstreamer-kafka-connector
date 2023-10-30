package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;

public class ItemTemplate<K, V> implements ItemTemplateInterface<K, V> {

    private static final Pattern ITEM_TEMPLATE = Pattern.compile("([a-zA-Z0-9_-]+)(-\\$\\{(.*)\\})?");

    private static final Pattern SELECTORS = Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?");

    private final BasicItem core;

    private final String topic;

    private final RecordInspector<K, V> inspector;

    private ItemTemplate(
            String topic,
            String prefix,
            RecordInspector<K, V> inspector) {
        this.topic = topic;
        this.inspector = inspector;
        this.core = new BasicItem(prefix, new HashSet<>(inspector.names()));
    }

    @Override
    public Item expand(ConsumerRecord<K, V> record) {
        List<Value> replaced = inspector.inspect(record);
        return new Item("", core.prefix(), replaced);
    }

    public String topic() {
        return topic;
    }

    @Override
    public String prefix() {
        return core.prefix();
    }

    @Override
    public Set<String> schemas() {
        return core.keys();
    }

    @Override
    public MatchResult match(Item other) {
        return core.matchStructure(other.core());
    }

    public RecordInspector<K,V> inspector() {
        return inspector;
    }

    static public <K, V> ItemTemplate<K, V> makeNew(
            String topic,
            String template,
            RecordInspector.Builder<K, V> inspectorBuilder) {
        Objects.requireNonNull(topic, "Invalid topic");
        Objects.requireNonNull(template, "Invalid template");
        Matcher matcher = ITEM_TEMPLATE.matcher(template);
        if (matcher.matches()) {
            String prefix = matcher.group(1);
            String selectors = matcher.group(3);
            if (selectors != null) {
                Matcher m = SELECTORS.matcher(selectors);
                int previousEnd = 0;
                while (m.find()) {
                    if (m.start() != previousEnd) {
                        break;
                    }
                    String name = m.group(2);
                    String expression = m.group(3);
                    inspectorBuilder.instruct(name, expression);
                    previousEnd = m.end();
                }
                if (previousEnd < selectors.length()) {
                    throw new RuntimeException("Invalid selector expression");
                }
            }
            return new ItemTemplate<>(topic, prefix, inspectorBuilder.build());
        }

        throw new RuntimeException("Invalid template");
    }
}
