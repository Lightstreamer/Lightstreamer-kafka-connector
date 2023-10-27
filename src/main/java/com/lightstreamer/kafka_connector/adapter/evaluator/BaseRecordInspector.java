package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lightstreamer.kafka_connector.adapter.consumers.ConsumerLoop;

class BaseRecordInspector<K, V> implements RecordInspector<K, V> {

    protected static Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    private final List<Selector<ConsumerRecord<?, ?>>> infoSelectors;

    private final List<Selector<K>> keySelectors;

    private final List<Selector<V>> valueSelectors;

    private final int valueSize;

    public BaseRecordInspector(
            List<Selector<ConsumerRecord<?, ?>>> infoSelectors,
            List<Selector<K>> ek,
            List<Selector<V>> ev) {
        this.infoSelectors = infoSelectors;
        this.keySelectors = ek;
        this.valueSelectors = ev;
        valueSize = infoSelectors.size() + valueSelectors.size() + keySelectors.size();
    }

    @Override
    public List<Selector<K>> keySelectors() {
        return keySelectors;
    }

    @Override
    public List<Selector<V>> valueSelectors() {
        return valueSelectors;
    }

    @Override
    public List<String> names() {
        List<String> list = Stream.concat(
                keySelectors.stream().map(Selector::name),
                Stream.concat(valueSelectors.stream().map(Selector::name),
                        infoSelectors.stream().map(Selector::name)))
                .toList();
        return list;
    }

    @Override
    public List<Value> inspect(ConsumerRecord<K, V> record) {
        Value[] values = new Value[valueSize];
        int c = 0;
        for (Selector<ConsumerRecord<?, ?>> infoSelector : infoSelectors) {
            Value value = infoSelector.extract(record);
            values[c++] = value;
        }
        for (Selector<? super K> keySelector : keySelectors) {
            Value value = keySelector.extract(record.key());
            log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
            values[c++] = value;
        }
        for (Selector<? super V> valueSelector : valueSelectors) {
            Value value = valueSelector.extract(record.value());
            log.debug("Extracted <{}> -> <{}>", value.name(), value.text());
            values[c++] = value;
        }
        return Arrays.asList(values);
    }

    public static RecordInspector<?, ?> makeInspector(SelectorSupplier<?> ek, SelectorSupplier<?> ev) {
        return makeInspectorKV(ek, ev);

    }

    static <K, V> RecordInspector<K, V> makeInspectorKV(
            SelectorSupplier<K> ek2,
            SelectorSupplier<V> ev2) {
        return new BaseRecordInspector.Builder<>(ek2, ev2).build();
    }
}
