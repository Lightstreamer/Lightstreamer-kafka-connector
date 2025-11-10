
/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.connect;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig.RECORD_EXTRACTION_ERROR_STRATEGY;

import static org.junit.Assert.assertThrows;

import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.FailureException;
import com.lightstreamer.adapters.remote.SubscriptionException;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.connect.Fakes.FakeSinkContext;
import com.lightstreamer.kafka.connect.Fakes.FakeSinkContext.FakeErrantRecordReporter;
import com.lightstreamer.kafka.connect.StreamingDataAdapter.DownstreamUpdater;
import com.lightstreamer.kafka.connect.config.LightstreamerConnectorConfig;
import com.lightstreamer.kafka.test_utils.Mocks.RemoteTestEventListener;
import com.lightstreamer.kafka.test_utils.Mocks.UpdateCall;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class StreamingDataAdapterTest {

    static Map<String, String> basicConfig(Map<String, String> overriding) {
        Map<String, String> config = new HashMap<>();
        config.put(LightstreamerConnectorConfig.LIGHTSTREAMER_PROXY_ADAPTER_ADDRESS, "host:6661");
        config.put(LightstreamerConnectorConfig.TOPIC_MAPPINGS, "topic:item1,item2");
        config.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE}");
        config.putAll(overriding);
        return config;
    }

    static LightstreamerConnectorConfig basicConnectorConfig(Map<String, String> overriding) {
        return new LightstreamerConnectorConfig(basicConfig(overriding));
    }

    static StreamingDataAdapter newAdapter() {
        return newAdapter(false, Collections.emptyMap());
    }

    static StreamingDataAdapter newAdapter(boolean withReporter) {
        return newAdapter(withReporter, Collections.emptyMap());
    }

    static StreamingDataAdapter newAdapter(DownstreamUpdater nopUpdater) {
        return newAdapter(false, Collections.emptyMap(), nopUpdater);
    }

    static StreamingDataAdapter newAdapter(Map<String, String> overriding) {
        return newAdapter(false, overriding);
    }

    static StreamingDataAdapter newAdapter(
            boolean withReporter, Map<String, String> overriding, DownstreamUpdater nopUpdater) {
        return new StreamingDataAdapter(
                DataAdapterConfigurator.configure(
                        basicConnectorConfig(overriding), new FakeSinkContext(withReporter)),
                nopUpdater);
    }

    static StreamingDataAdapter newAdapter(boolean withReporter, Map<String, String> overriding) {
        return new StreamingDataAdapter(
                DataAdapterConfigurator.configure(
                        basicConnectorConfig(overriding), new FakeSinkContext(withReporter)));
    }

    static StreamingDataAdapter newAdapter(
            FakeSinkContext sinkContext, Map<String, String> overriding) {
        return new StreamingDataAdapter(
                DataAdapterConfigurator.configure(basicConnectorConfig(overriding), sinkContext));
    }

    @Test
    void shouldCreate() throws SubscriptionException {
        StreamingDataAdapter adapter = newAdapter();
        assertThat(adapter.getInitParameters()).isEmpty();
        assertThat(adapter.getErrantRecordReporter()).isNull();
        assertThat(adapter.isSnapshotAvailable("anItem")).isFalse();
        assertThat(adapter.getErrantRecordReporter()).isNull();
    }

    @Test
    void shouldCreateWithNoErrantReporter() {
        StreamingDataAdapter adapter = newAdapter(true);
        assertThat(adapter.getErrantRecordReporter()).isNotNull();
    }

    @Test
    void shouldInit() throws DataProviderException {
        StreamingDataAdapter adapter = newAdapter();
        adapter.init(Map.of("param1", "value1"), "path/to/config/file");
        assertThat(adapter.getInitParameters()).containsExactly("param1", "value1");
    }

    @Test
    void shouldSetItemEventListener() {
        StreamingDataAdapter adapter = newAdapter();
        RemoteTestEventListener eventListener = new RemoteTestEventListener();
        adapter.setListener(eventListener);
        assertThat(adapter.getEventListener()).isNotNull();
    }

    @Test
    void shouldSubscribeAndUnsubscribe() throws SubscriptionException, FailureException {
        StreamingDataAdapter adapter = newAdapter();
        assertThat(adapter.getUpdater()).isSameInstanceAs(StreamingDataAdapter.NOP_UPDATER);

        adapter.subscribe("item1");
        Optional<SubscribedItem> subscribedItem = adapter.getSubscribedItem("item1");
        assertThat(subscribedItem.get().schema().name()).isEqualTo("item1");
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(1);
        assertThat(adapter.getUpdater()).isNotSameInstanceAs(StreamingDataAdapter.NOP_UPDATER);

        adapter.subscribe("item2");
        Optional<SubscribedItem> subscribedItem2 = adapter.getSubscribedItem("item2");
        assertThat(subscribedItem2.get().schema().name()).isEqualTo("item2");
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(2);
        assertThat(adapter.getUpdater()).isNotSameInstanceAs(StreamingDataAdapter.NOP_UPDATER);

        adapter.unsubscribe("item1");
        assertThat(adapter.getSubscribedItem("item1")).isEmpty();
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(1);
        assertThat(adapter.getUpdater()).isNotSameInstanceAs(StreamingDataAdapter.NOP_UPDATER);

        adapter.unsubscribe("item2");
        assertThat(adapter.getSubscribedItem("item2")).isEmpty();
        assertThat(adapter.getCurrentItemsCount()).isEqualTo(0);
        assertThat(adapter.getUpdater()).isSameInstanceAs(StreamingDataAdapter.NOP_UPDATER);
    }

    @Test
    public void shouldNotSubscribeToNotAllowedItems() {
        StreamingDataAdapter adapter = newAdapter();
        SubscriptionException se1 =
                assertThrows(SubscriptionException.class, () -> adapter.subscribe("anItem"));
        assertThat(se1.getMessage()).isEqualTo("Item does not match any defined item templates");

        SubscriptionException se2 =
                assertThrows(SubscriptionException.class, () -> adapter.subscribe("@"));
        assertThat(se2.getMessage()).isEqualTo("Invalid Item");
    }

    @Test
    public void shouldNotUnsubscribeFromNotExistingItem() {
        StreamingDataAdapter adapter = newAdapter();
        assertThrows(SubscriptionException.class, () -> adapter.unsubscribe("item"));
    }

    static SinkRecord mkRecord() {
        return mkRecord("topic", 1, 1);
    }

    static SinkRecord mkRecord(String topic) {
        return mkRecord(topic, 1, 1);
    }

    static SinkRecord mkRecord(int value, int offset) {
        return mkRecord("topic", value, offset);
    }

    static SinkRecord mkRecord(String topic, int value, int offset) {
        return new SinkRecord(topic, 1, null, null, Schema.INT8_SCHEMA, value, offset);
    }

    static SinkRecord mkRecord(Map<String, Integer> value, int offset) {
        return mkRecord("topic", value, offset);
    }

    static SinkRecord mkRecord(String topic, Map<String, Integer> value, int offset) {
        Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT8_SCHEMA).build();
        return new SinkRecord(topic, 1, null, null, valueSchema, value, offset);
    }

    @Test
    public void shouldNotStreamEventsWithNoSubscription() {
        AtomicInteger counter = new AtomicInteger();

        // Initialize a StreamingAdapter with an NOP Updater to track events that will not be
        // forwarded to any items.
        StreamingDataAdapter adapter = newAdapter(records -> counter.incrementAndGet());
        RemoteTestEventListener eventsListener = new RemoteTestEventListener();
        adapter.setListener(eventsListener);
        adapter.sendRecords(Collections.singleton(mkRecord()));

        // The NOP Updater has been notified with the event.
        assertThat(counter.get()).isEqualTo(1);
    }

    static Stream<Arguments> events() {
        return Stream.of(
                Arguments.of(List.of(mkRecord(1, 1)), 2, 2),
                Arguments.of(List.of(mkRecord(10, 1), mkRecord(44, 2)), 3, 4));
    }

    @ParameterizedTest
    @MethodSource("events")
    public void shouldStreamEvents(
            Collection<SinkRecord> records, int expectedOffset, int expectedDeliveredEvents)
            throws SubscriptionException, FailureException {
        StreamingDataAdapter adapter = newAdapter();
        RemoteTestEventListener eventsListener = new RemoteTestEventListener();
        adapter.setListener(eventsListener);
        adapter.subscribe("item1");
        adapter.subscribe("item2");
        assertThat(adapter.getCurrentOffsets()).isEmpty();
        adapter.sendRecords(records);

        // Since we have subscribed to two items, we expect the total number of real-time updates to
        // be twice the number of records.
        assertThat(eventsListener.getRealtimeUpdateCount()).isEqualTo(expectedDeliveredEvents);

        // Verify that the next offset for the involved partition
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = adapter.getCurrentOffsets();
        assertThat(currentOffsets).hasSize(1);
        OffsetAndMetadata offsetAndMetadata = currentOffsets.get(new TopicPartition("topic", 1));
        assertThat(offsetAndMetadata.offset()).isEqualTo(expectedOffset);
    }

    @Test
    public void shouldIgnoreAndContinue() throws SubscriptionException, FailureException {
        int CURRENT_OFFSET = 5;
        int NEXT_OFFSET = 6;
        Map<String, String> otherSettings = new HashMap<>();

        // Configure the IGNORE_AND_CONTINUE error strategy.
        otherSettings.put(RECORD_EXTRACTION_ERROR_STRATEGY, "IGNORE_AND_CONTINUE");
        // This entry will trigger a ValueException for records that are missing the 'attrib' field.
        otherSettings.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE.attrib}");

        // Create new StreamingDataAdapter with ErrantRecordReporter enabled.
        FakeSinkContext sinkContext = new FakeSinkContext(true);
        StreamingDataAdapter adapter = newAdapter(sinkContext, otherSettings);
        RemoteTestEventListener eventListener = new RemoteTestEventListener();
        adapter.setListener(eventListener);
        adapter.subscribe("item1");

        SinkRecord record = mkRecord(213, CURRENT_OFFSET);
        assertThat(adapter.getCurrentOffsets()).isEmpty();
        adapter.sendRecords(Collections.singleton(record));

        // Verify that the ItemEventListener has NOT been updated.
        assertThat(eventListener.getRealtimeUpdateCount()).isEqualTo(0);

        // Verify that the Reporter has NOT been involved.
        FakeErrantRecordReporter reporter =
                (FakeErrantRecordReporter) sinkContext.errantRecordReporter();
        assertThat(reporter.caughtError).isNull();
        assertThat(reporter.record).isNull();

        // Verify that the next offset for the involved partition.
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = adapter.getCurrentOffsets();
        assertThat(currentOffsets).hasSize(1);
        OffsetAndMetadata offsetAndMetadata = currentOffsets.get(new TopicPartition("topic", 1));
        assertThat(offsetAndMetadata.offset()).isEqualTo(NEXT_OFFSET);
    }

    @Test
    public void shouldSkipFailedMapping() throws SubscriptionException, FailureException {
        int CURRENT_OFFSET = 5;
        int NEXT_OFFSET = 6;
        Map<String, String> otherSettings = new HashMap<>();

        // Configure the IGNORE_AND_CONTINUE error strategy.
        otherSettings.put(RECORD_EXTRACTION_ERROR_STRATEGY, "IGNORE_AND_CONTINUE");
        // This setting will allow failed field to be omitted from the updates set to clients.
        otherSettings.put(LightstreamerConnectorConfig.RECORD_MAPPINGS_SKIP_FAILED_ENABLE, "true");
        // This entry will trigger a ValueException for records that are missing the 'attrib' field.
        otherSettings.put(
                LightstreamerConnectorConfig.RECORD_MAPPINGS,
                "field1:#{VALUE.not_valid_attrib},field2:#{VALUE.attrib}");

        // Create new StreamingDataAdapter with ErrantRecordReporter enabled.
        FakeSinkContext sinkContext = new FakeSinkContext(true);
        StreamingDataAdapter adapter = newAdapter(sinkContext, otherSettings);
        RemoteTestEventListener eventListener = new RemoteTestEventListener();
        adapter.setListener(eventListener);
        adapter.subscribe("item1");

        SinkRecord record = mkRecord(Map.of("attrib", 213), CURRENT_OFFSET);
        assertThat(adapter.getCurrentOffsets()).isEmpty();
        adapter.sendRecords(Collections.singleton(record));

        // Verify that the ItemEventListener has been updated only with the non-failed field.
        assertThat(eventListener.getRealtimeUpdates())
                .containsExactly(new UpdateCall("item1", Map.of("field2", "213"), false));

        // Verify that the Reporter has NOT been involved.
        FakeErrantRecordReporter reporter =
                (FakeErrantRecordReporter) sinkContext.errantRecordReporter();
        assertThat(reporter.caughtError).isNull();
        assertThat(reporter.record).isNull();

        // Verify that the next offset for the involved partition.
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = adapter.getCurrentOffsets();
        assertThat(currentOffsets).hasSize(1);
        OffsetAndMetadata offsetAndMetadata = currentOffsets.get(new TopicPartition("topic", 1));
        assertThat(offsetAndMetadata.offset()).isEqualTo(NEXT_OFFSET);
    }

    @Test
    public void shouldForwardToDLQ() throws SubscriptionException, FailureException {
        int CURRENT_OFFSET = 1;
        int NEXT_OFFSET = 3;
        Map<String, String> otherSettings = new HashMap<>();

        // Configure the FORWARD_TO_DLQ error strategy.
        otherSettings.put(RECORD_EXTRACTION_ERROR_STRATEGY, "FORWARD_TO_DLQ");
        // This entry will trigger a ValueException for records that are missing the 'attrib' field.
        otherSettings.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE.attrib}");

        // Create new StreamingDataAdapter with ErrantRecordReporter enabled
        FakeSinkContext sinkContext = new FakeSinkContext(true);
        StreamingDataAdapter adapter = newAdapter(sinkContext, otherSettings);
        RemoteTestEventListener eventListener = new RemoteTestEventListener();
        adapter.setListener(eventListener);
        adapter.subscribe("item1");

        SinkRecord record1 = mkRecord(Map.of("attrib", 213), CURRENT_OFFSET);
        SinkRecord record2 = mkRecord(Map.of("noMappedAttrib", 214), CURRENT_OFFSET + 1);
        assertThat(adapter.getCurrentOffsets()).isEmpty();
        adapter.sendRecords(List.of(record1, record2));

        // Verify that the ItemEventListener has been updated only with the first record.
        assertThat(eventListener.getRealtimeUpdates())
                .containsExactly(new UpdateCall("item1", Map.of("field1", "213"), false));

        // Verify that the Reporter has been updated with the second record and the related thrown
        // exception.
        FakeErrantRecordReporter reporter =
                (FakeErrantRecordReporter) sinkContext.errantRecordReporter();
        assertThat(reporter.caughtError).isInstanceOf(ValueException.class);
        assertThat(reporter.record).isSameInstanceAs(record2);

        // Verify that the next offset for the involved partition.
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = adapter.getCurrentOffsets();
        assertThat(currentOffsets).hasSize(1);
        OffsetAndMetadata offsetAndMetadata = currentOffsets.get(new TopicPartition("topic", 1));
        assertThat(offsetAndMetadata.offset()).isEqualTo(NEXT_OFFSET);
    }

    @Test
    public void shouldTerminateTaskDueToMissingReporter()
            throws SubscriptionException, FailureException {
        int CURRENT_OFFSET = 1;
        Map<String, String> otherSettings = new HashMap<>();

        // Configure the FORWARD_TO_DLQ error strategy.
        otherSettings.put(RECORD_EXTRACTION_ERROR_STRATEGY, "FORWARD_TO_DLQ");
        // This entry will trigger a ValueException for records that are missing the 'attrib' field.
        otherSettings.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE.attrib}");

        // Create new StreamingDataAdapter with no ErrantRecordReporter enabled.
        FakeSinkContext sinkContext = new FakeSinkContext(false);
        StreamingDataAdapter adapter = newAdapter(sinkContext, otherSettings);
        RemoteTestEventListener eventListener = new RemoteTestEventListener();
        adapter.setListener(eventListener);
        adapter.subscribe("item1");

        SinkRecord record = mkRecord(213, CURRENT_OFFSET);
        assertThat(adapter.getCurrentOffsets()).isEmpty();
        ConnectException ce =
                assertThrows(
                        ConnectException.class,
                        () -> adapter.sendRecords(Collections.singleton(record)));
        assertThat(ce.getLocalizedMessage()).isEqualTo("No DQL, terminating task");
        assertThat(ce.getCause()).isInstanceOf(ValueException.class);

        // Verify that the ItemEventListener has NOT been updated.
        assertThat(eventListener.getRealtimeUpdates()).isEmpty();

        // Verify that the next offset for the involved partition has NOT been updated
        assertThat(adapter.getCurrentOffsets()).isEmpty();
    }

    @Test
    public void shouldTerminateTask() throws SubscriptionException, FailureException {
        int CURRENT_OFFSET = 1;
        Map<String, String> otherSettings = new HashMap<>();

        // Configure the TERMINATE_TASK error strategy.
        otherSettings.put(RECORD_EXTRACTION_ERROR_STRATEGY, "TERMINATE_TASK");
        // This entry will trigger a ValueException for records that are missing the 'attrib' field.
        otherSettings.put(LightstreamerConnectorConfig.RECORD_MAPPINGS, "field1:#{VALUE.attrib}");

        // Create new StreamingDataAdapter with no ErrantRecordReporter enabled.
        FakeSinkContext sinkContext = new FakeSinkContext(false);
        StreamingDataAdapter adapter = newAdapter(sinkContext, otherSettings);
        RemoteTestEventListener eventListener = new RemoteTestEventListener();
        adapter.setListener(eventListener);
        adapter.subscribe("item1");

        SinkRecord record = mkRecord(213, CURRENT_OFFSET);
        assertThat(adapter.getCurrentOffsets()).isEmpty();
        ConnectException ce =
                assertThrows(
                        ConnectException.class,
                        () -> adapter.sendRecords(Collections.singleton(record)));
        assertThat(ce.getLocalizedMessage()).isEqualTo("Terminating task");
        assertThat(ce.getCause()).isInstanceOf(ValueException.class);

        // Verify that the ItemEventListener has NOT been updated.
        assertThat(eventListener.getRealtimeUpdates()).isEmpty();

        // Verify that the next offset for the involved partition has NOT been updated.
        assertThat(adapter.getCurrentOffsets()).isEmpty();
    }

    @Test
    public void shouldSaveOffsets() {
        StreamingDataAdapter adapter = newAdapter();
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = adapter.getCurrentOffsets();
        assertThat(currentOffsets).isEmpty();

        SinkRecord r1 = mkRecord(4, 5);
        adapter.saveOffsets(r1);
        assertThat(currentOffsets.get(new TopicPartition("topic", 1)).offset()).isEqualTo(6);

        SinkRecord r2 = mkRecord(10, 6);
        adapter.saveOffsets(r2);
        assertThat(currentOffsets.get(new TopicPartition("topic", 1)).offset()).isEqualTo(7);

        adapter.saveOffsets(mkRecord("anotherTopic", 10, 1));
        assertThat(currentOffsets.get(new TopicPartition("anotherTopic", 1)).offset()).isEqualTo(2);

        adapter.saveOffsets(mkRecord("anotherTopic", 10, 2));
        assertThat(currentOffsets.get(new TopicPartition("anotherTopic", 1)).offset()).isEqualTo(3);
    }

    @Test
    public void shouldPreCommit() {
        StreamingDataAdapter adapter = newAdapter();
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = adapter.getCurrentOffsets();
        assertThat(currentOffsets).isEmpty();

        assertThat(adapter.preCommit(Collections.emptyMap())).isSameInstanceAs(currentOffsets);
    }
}
