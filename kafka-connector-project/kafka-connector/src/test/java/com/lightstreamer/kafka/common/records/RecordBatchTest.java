
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.common.records;

import static com.google.common.truth.Truth.assertThat;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.test_utils.Records;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class RecordBatchTest {

    @FunctionalInterface
    interface ThrowableRunnable {
        void run() throws Exception;
    }

    private KafkaRecord.DeserializerPair<String, String> deserializerPair =
            new KafkaRecord.DeserializerPair<>(String().deserializer(), String().deserializer());

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateBatchFromDeferred(boolean joinable) {
        int partitions = 2;
        int totalRecords = 20;
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", totalRecords, List.of(), partitions);
        RecordBatch<String, String> eager =
                RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, joinable);

        if (joinable) {
            assertThat(eager).isInstanceOf(JoinableRecordBatch.class);
        } else {
            assertThat(eager).isInstanceOf(NotifyingRecordBatch.class);
        }

        // Verify total size
        assertThat(eager.count()).isEqualTo(totalRecords);
        assertThat(eager.isEmpty()).isFalse();

        List<KafkaRecord<String, String>> records = eager.getRecords();

        // Verify records per partition
        for (int p = 0; p < partitions; p++) {
            final int partition = p;
            assertThat(records.stream().filter(k -> k.partition() == partition).count())
                    .isEqualTo(totalRecords / partitions);
        }

        // Verify record content and order are preserved
        int index = 0;
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < totalRecords / partitions; o++) {
                KafkaRecord<String, String> record = records.get(index++);
                assertThat(record.partition()).isEqualTo(p);
                assertThat(record.offset()).isEqualTo(o);
                assertThat(record.topic()).isEqualTo("topic");
                assertThat(record.getBatch()).isSameInstanceAs(eager);
            }
        }

        // Verify deferred deserialization (key and value are cached after first access)
        KafkaRecord<String, String> firstRecord = records.get(0);
        String key = firstRecord.key();
        assertThat(firstRecord.key()).isSameInstanceAs(key);
        String value = firstRecord.value();
        assertThat(firstRecord.value()).isSameInstanceAs(value);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateBatchFromEager(boolean joinable) {
        int partitions = 2;
        int totalRecords = 20;
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", totalRecords, List.of(), partitions);
        RecordBatch<String, String> eager =
                RecordBatch.batchFromEager(consumerRecords, deserializerPair, joinable);

        if (joinable) {
            assertThat(eager).isInstanceOf(JoinableRecordBatch.class);
        } else {
            assertThat(eager).isInstanceOf(NotifyingRecordBatch.class);
        }

        // Verify total size
        assertThat(eager.count()).isEqualTo(totalRecords);
        assertThat(eager.isEmpty()).isFalse();

        List<KafkaRecord<String, String>> records = eager.getRecords();

        // Verify records per partition
        for (int p = 0; p < partitions; p++) {
            final int partition = p;
            assertThat(records.stream().filter(k -> k.partition() == partition).count())
                    .isEqualTo(totalRecords / partitions);
        }

        // Verify record content and order are preserved
        int index = 0;
        for (int p = 0; p < partitions; p++) {
            for (int o = 0; o < totalRecords / partitions; o++) {
                KafkaRecord<String, String> record = records.get(index++);
                assertThat(record.partition()).isEqualTo(p);
                assertThat(record.offset()).isEqualTo(o);
                assertThat(record.topic()).isEqualTo("topic");
                assertThat(record.getBatch()).isSameInstanceAs(eager);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateEmptyBatch(boolean joinable) {
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", 0, List.of("a", "b"));

        RecordBatch<String, String> emptyBatchFromDeferred =
                RecordBatch.batchFromDeferred(consumerRecords, deserializerPair, joinable);
        assertThat(emptyBatchFromDeferred.count()).isEqualTo(0);
        assertThat(emptyBatchFromDeferred.isEmpty()).isTrue();
        assertThat(emptyBatchFromDeferred.getRecords()).isEmpty();

        RecordBatch<String, String> emptyBatchFromEager =
                RecordBatch.batchFromEager(consumerRecords, deserializerPair, joinable);
        assertThat(emptyBatchFromEager.count()).isEqualTo(0);
        assertThat(emptyBatchFromEager.isEmpty()).isTrue();
        assertThat(emptyBatchFromEager.getRecords()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotifyBatchCompletion(boolean joinable) {
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", 5, List.of("a", "b"));
        RecordBatch<String, String> batch =
                RecordBatch.batchFromEager(consumerRecords, deserializerPair, joinable);

        AtomicBoolean completed = new AtomicBoolean(false);
        RecordBatch.RecordBatchListener listener = recordBatch -> completed.set(true);
        assertThat(completed.get()).isFalse();

        // Process records one by one
        for (int i = 0; i < 5; i++) {
            assertThat(completed.get()).isFalse();
            batch.recordProcessed(listener);
        }

        // After all records are processed, completion should be notified
        assertThat(completed.get()).isTrue();
    }

    @Test
    public void shouldJoinDefaultBatch() {
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", 5, List.of("a", "b"));

        RecordBatch<String, String> batch =
                RecordBatch.batchFromEager(consumerRecords, deserializerPair, true);

        // Process records one by one
        for (int i = 0; i < 5; i++) {
            batch.recordProcessed(recordBatch -> {});
        }

        // Joining should complete immediately as all records are processed
        batch.join();
    }

    @Test
    public void shouldBlockJoinUntilAllRecordsProcessed() throws Exception {
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", 5, List.of("a", "b"));
        RecordBatch<String, String> batch =
                RecordBatch.batchFromEager(consumerRecords, deserializerPair, true);

        // Start join on a separate thread
        CompletableFuture<Void> joinFuture = CompletableFuture.runAsync(batch::join);

        // Verify join is blocking - should timeout when waiting for completion
        assertThat(shouldTimeout(() -> joinFuture.get(100, TimeUnit.MILLISECONDS))).isTrue();

        // Process 4 records - join should still be waiting
        for (int i = 0; i < 4; i++) {
            batch.recordProcessed(b -> {});
            assertThat(shouldTimeout(() -> joinFuture.get(50, TimeUnit.MILLISECONDS))).isTrue();
        }

        // Process final record - join should complete
        batch.recordProcessed(b -> {});
        // Verify join completes without timeout
        assertThat(shouldTimeout(() -> joinFuture.get(1, TimeUnit.SECONDS))).isFalse();
    }

    @Test
    public void shouldNotBlockJoinForNonJoinableBatch() throws Exception {
        ConsumerRecords<byte[], byte[]> consumerRecords =
                Records.generateRecords("topic", 5, List.of("a", "b"));
        RecordBatch<String, String> batch =
                RecordBatch.batchFromEager(consumerRecords, deserializerPair, false);

        // Start join on a separate thread
        CompletableFuture<Void> joinFuture = CompletableFuture.runAsync(batch::join);

        // Even without processing any records, join should still return immediately
        joinFuture.get(10, TimeUnit.MILLISECONDS);
    }

    private boolean shouldTimeout(ThrowableRunnable operation) {
        try {
            operation.run();
            return false; // No timeout occurred
        } catch (TimeoutException | InterruptedException e) {
            return true; // Timeout or interrupt occurred as expected
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Stream<Arguments> consumerRecords() {
        return Stream.of(

                // Target size smaller than actual records
                Arguments.of(0, Records.generateRecords("topic", 1, List.of("a", "b"))),
                Arguments.of(1, Records.generateRecords("topic", 2, List.of("a", "b"))),
                // Target size larger than actual records
                Arguments.of(2, Records.generateRecords("topic", 1, List.of("a", "b"))),
                Arguments.of(3, Records.generateRecords("topic", 2, List.of("a", "b"))));
    }

    @ParameterizedTest
    @MethodSource("consumerRecords")
    public void shouldNotValidateBatchConstruction(
            int targetSize, ConsumerRecords<byte[], byte[]> consumerRecords) {
        NotifyingRecordBatch<String, String> batch = new NotifyingRecordBatch<>(targetSize);
        for (var record : consumerRecords) {
            batch.addDeferredRecord(record, deserializerPair);
        }
        assertThrows(IllegalStateException.class, batch::validate);
    }
}
