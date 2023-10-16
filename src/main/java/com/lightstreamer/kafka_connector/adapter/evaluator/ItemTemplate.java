package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

public class ItemTemplate<R> extends BasicItem {

    private static final Pattern PREFIX = Pattern.compile("([a-zA-Z0-9_-]+)(-\\$\\{(.*)\\})?");

    private static final Pattern SELECTORS = Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?");

    public ItemTemplate(String prefix, List<? extends ValueSelector<R>> selectors) {
        super(prefix, selectors);
    }

    public Item expand(R record) {
        List<Value> replaced = new ArrayList<>();
        for (ValueSchema selector : this.schemas) {
            replaced.add(((ValueSelector<R>) selector).extract(record));
        }

        return new Item("", prefix(), replaced);
    }

    static public <V> ItemTemplate<V> makeNew(@Nonnull String template,
            BiFunction<String, String, ValueSelector<V>> v) {
        Objects.requireNonNull(template, "Invalid template");
        Matcher matcher = PREFIX.matcher(template);
        if (matcher.matches()) {
            List<ValueSelector<V>> valueSelectors = new ArrayList<>();
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
            return new ItemTemplate<>(prefix, valueSelectors);
        }

        throw new RuntimeException("Invalid template");
    }

    static public <V> ItemTemplate<V> makeNewOLD(String template, BiFunction<String, String, ValueSelector<V>> v) {
        int start = template.indexOf("${");
        if (start != -1) {
            int end = template.lastIndexOf("}");
            if (end != -1) {
                String prefix = template.substring(0, start);
                String expr = template.substring(start + 2, end);
                String[] terms = expr.split(",");
                List<ValueSelector<V>> selectors = new ArrayList<>();
                for (int i = 0; i < terms.length; i++) {
                    String name = terms[i].substring(0, terms[i].indexOf('='));
                    String expression = terms[i].substring(terms[i].indexOf('=') + 1);
                    selectors.add(v.apply(name, expression));
                }
                return new ItemTemplate<V>(prefix, selectors);
            }
        }
        return new ItemTemplate<V>(template, Collections.emptyList());
    }
}
