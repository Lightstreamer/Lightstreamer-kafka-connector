package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseRecordInspector<K, V> implements RecordInspector<K, V> {

    protected static Logger log = LoggerFactory.getLogger(BaseRecordInspector.class);

    private final List<Selector<ConsumerRecord<?, ?>>> infoSelectors;

    private final List<Selector<K>> keySelectors;

    private final List<Selector<V>> valueSelectors;

    private final int valueSize;

    public BaseRecordInspector(
            List<Selector<ConsumerRecord<?, ?>>> is,
            List<Selector<K>> ks,
            List<Selector<V>> vs) {
        this.infoSelectors = is;
        this.keySelectors = ks;
        this.valueSelectors = vs;
        valueSize = is.size() + valueSelectors.size() + keySelectors.size();
    }


    @Override
    public List<Selector<ConsumerRecord<?,?>>> infoSelectors() {
        return infoSelectors;
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
        Stream<String> infoNames = infoSelectors.stream().map(Selector::name);
        Stream<String> keyNames = keySelectors.stream().map(Selector::name);
        Stream<String> valueNames = valueSelectors.stream().map(Selector::name);

        return Stream.of(infoNames, keyNames, valueNames).flatMap(i -> i).toList();
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
