package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.lightstreamer.kafka_connector.adapter.evaluator.BasicItem.MatchResult;

public class ItemTemplate<R> implements ItemTemplateInterface<R> {

    private static final Pattern ITEM_TEMPLATE = Pattern.compile("([a-zA-Z0-9_-]+)(-\\$\\{(.*)\\})?");

    private static final Pattern SELECTORS = Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?");

    private final BasicItem core;

    private final List<? extends ValueSelector<String, R>> selectors;

    private final String topic;

    private ItemTemplate(String topic, String prefix, List<? extends ValueSelector<String, R>> selectors) {
        this.topic = topic;
        this.core = new BasicItem(prefix, new HashSet<>(selectors.stream().map(ValueSelector::name).toList()));
        this.selectors = selectors;
    }

    @Override
    public Item expand(ConsumerRecord<String, R> record) {
        List<Value> replaced = new ArrayList<>();
        for (ValueSelector<String, R> selector : this.selectors) {
            replaced.add(selector.extract(record));
        }

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

    @Override
    public List<? extends ValueSelector<String, R>> valueSelectors() {
        return selectors;
    }

    static public <V> ItemTemplate<V> makeNew(String topic, String template,
            BiFunction<String, String, ValueSelector<String, V>> v) {
        Objects.requireNonNull(topic, "Invalid topic");
        Objects.requireNonNull(template, "Invalid template");
        Matcher matcher = ITEM_TEMPLATE.matcher(template);
        if (matcher.matches()) {
            List<ValueSelector<String, V>> valueSelectors = new ArrayList<>();
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
                    valueSelectors.add(v.apply(name, expression));
                    previousEnd = m.end();
                }
                if (previousEnd < selectors.length()) {
                    throw new RuntimeException("Invalid selector expression");
                }
            }
            return new ItemTemplate<>(topic, prefix, valueSelectors);
        }

        throw new RuntimeException("Invalid template");
    }

    // static public <V> ItemTemplateInterface<V> makeNewOLD(String template,
    // BiFunction<String, String, ValueSelector<V>> v) {
    // int start = template.indexOf("${");
    // if (start != -1) {
    // int end = template.lastIndexOf("}");
    // if (end != -1) {
    // String prefix = template.substring(0, start);
    // String expr = template.substring(start + 2, end);
    // String[] terms = expr.split(",");
    // List<ValueSelector<V>> selectors = new ArrayList<>();
    // for (int i = 0; i < terms.length; i++) {
    // String name = terms[i].substring(0, terms[i].indexOf('='));
    // String expression = terms[i].substring(terms[i].indexOf('=') + 1);
    // selectors.add(v.apply(name, expression));
    // }
    // return new ItemTemplate<V>(prefix, selectors);
    // }
    // }
    // return new ItemTemplate<V>(template, Collections.emptyList());
    // }
}
