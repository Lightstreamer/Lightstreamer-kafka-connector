
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import com.lightstreamer.interfaces.data.ItemEventListener;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.CommandMode;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.consumers.offsets.OffsetService;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;
import com.lightstreamer.kafka.common.monitors.Monitor;
import com.lightstreamer.kafka.common.records.KafkaRecord;
import com.lightstreamer.kafka.common.records.RecordBatch;

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Consumes {@link RecordBatch} instances, dispatching each record to a {@link RecordProcessor}
 * according to the configured error handling and ordering strategies.
 *
 * <p>Instances are created via a step builder starting from {@link #recordMapper(RecordMapper)} or
 * {@link #recordProcessor(RecordProcessor)}.
 *
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public interface RecordConsumer<K, V> {

    /** Strategy that determines the order in which records are dispatched to workers. */
    enum OrderStrategy {
        ORDER_BY_KEY(record -> Objects.toString(record.key(), null)),
        ORDER_BY_PARTITION(record -> record.topic() + "-" + record.partition()),
        UNORDERED(record -> null);

        private final Function<KafkaRecord<?, ?>, String> sequence;

        OrderStrategy(Function<KafkaRecord<?, ?>, String> sequence) {
            this.sequence = sequence;
        }

        String getSequence(KafkaRecord<?, ?> record) {
            return sequence.apply(record);
        }

        /**
         * Converts a configuration {@link RecordConsumeWithOrderStrategy} to the corresponding
         * {@code OrderStrategy}.
         *
         * @param strategy the configuration strategy to convert
         * @return the matching {@code OrderStrategy}
         */
        public static OrderStrategy from(RecordConsumeWithOrderStrategy strategy) {
            return switch (strategy) {
                case ORDER_BY_KEY -> ORDER_BY_KEY;
                case ORDER_BY_PARTITION -> ORDER_BY_PARTITION;
                case UNORDERED -> UNORDERED;
            };
        }
    }

    /**
     * Processes a single {@link KafkaRecord}, mapping it to subscribed items and dispatching
     * updates to the {@link ItemEventListener}.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    interface RecordProcessor<K, V> {

        /** Determines how updates are dispatched to subscribed items. */
        enum ProcessUpdatesType {
            DEFAULT,
            COMMAND,
            AUTO_COMMAND_MODE;

            /**
             * Returns whether records can be processed concurrently with this update type.
             *
             * @return {@code true} if concurrent processing is allowed, {@code false} otherwise
             */
            boolean allowConcurrentProcessing() {
                return this != COMMAND;
            }
        }

        /**
         * Processes a record as a realtime event.
         *
         * @param record the record to process
         * @throws ValueException if field extraction fails
         */
        default void process(KafkaRecord<K, V> record) throws ValueException {
            process(record, false);
        }

        /**
         * Processes a record, optionally delivering it as a snapshot event.
         *
         * @param record the record to process
         * @param isSnapshot {@code true} to deliver via snapshot, {@code false} for realtime
         * @throws ValueException if field extraction fails
         */
        void process(KafkaRecord<K, V> record, boolean isSnapshot) throws ValueException;

        /**
         * Sets the logger for this processor.
         *
         * @param logger the logger to use for diagnostic output
         */
        void useLogger(Logger logger);

        /**
         * Returns the update dispatch type for this processor.
         *
         * @return the {@link ProcessUpdatesType}
         */
        ProcessUpdatesType processUpdatesType();
    }

    /**
     * Builder step for configuring a {@link RecordProcessor}.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    public interface StartBuildingProcessor<K, V> {

        /**
         * Sets the subscribed items for the processor.
         *
         * @param subscribedItems the items to which records will be dispatched
         * @return the next builder step
         */
        WithSubscribedItems<K, V> subscribedItems(SubscribedItems subscribedItems);
    }

    /**
     * Builder step for setting the command mode strategy.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    interface WithSubscribedItems<K, V> {

        /**
         * Sets the {@link ItemEventListener} that receives dispatched updates.
         *
         * @param listener the event listener
         * @return the next builder step
         */
        WithEventListener<K, V> eventListener(ItemEventListener listener);
    }

    /**
     * Builder step for configuring a {@link RecordConsumer}.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    public interface WithEventListener<K, V> {

        /**
         * Sets the {@link OffsetService} for offset management.
         *
         * @param offsetService the offset service
         * @return the next builder step
         */
        WithOffsetService<K, V> offsetService(OffsetService offsetService);
    }

    /**
     * Builder step for setting the error handling strategy.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    interface WithOffsetService<K, V> {

        /**
         * Sets the strategy for handling record processing errors.
         *
         * @param errorHandlingStrategy the error handling strategy
         * @return the next builder step
         */
        WithOptionals<K, V> logger(Logger logger);
    }

    /**
     * Builder step for setting optional configuration parameters.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     */
    interface WithOptionals<K, V> {

        WithOptionals<K, V> errorStrategy(RecordErrorHandlingStrategy errorHandlingStrategy);

        WithOptionals<K, V> commandMode(CommandMode commandMode);

        /**
         * Enables or disables the catch-up phase for this consumer.
         *
         * <p>When enabled, records are initially delivered as snapshot events until {@link
         * #endCatchUp()} is called.
         *
         * @param catchUp {@code true} to enable catch-up mode, {@code false} otherwise
         * @return this builder step
         */
        WithOptionals<K, V> enableCatchUp(boolean catchUp);

        /**
         * Sets the number of worker threads for parallel processing.
         *
         * @param threads the number of threads
         * @return this builder step
         */
        WithOptionals<K, V> threads(int threads);

        /**
         * Sets the ordering strategy for record dispatch.
         *
         * @param orderStrategy the {@link OrderStrategy} to apply
         * @return this builder step
         */
        WithOptionals<K, V> ordering(OrderStrategy orderStrategy);

        /**
         * Indicates a preference for single-threaded processing regardless of thread count.
         *
         * @param singleThread {@code true} to prefer single-threaded execution
         * @return this builder step
         */
        WithOptionals<K, V> preferSingleThread(boolean singleThread);

        /**
         * Sets the {@link Monitor} for tracking consumer metrics.
         *
         * @param monitor the monitor
         * @return this builder step
         */
        WithOptionals<K, V> monitor(Monitor monitor);

        /**
         * Builds the {@link RecordConsumer} with the configured parameters.
         *
         * @return a new {@link RecordConsumer} instance
         */
        RecordConsumer<K, V> build();
    }

    /**
     * Starts the builder chain for creating a {@link RecordProcessor} and then a {@code
     * RecordConsumer}.
     *
     * @param <K> the type of the key in the Kafka record
     * @param <V> the type of the value in the Kafka record
     * @param mapper the record mapper for field extraction
     * @return the first step of the processor builder
     */
    public static <K, V> StartBuildingProcessor<K, V> recordMapper(RecordMapper<K, V> mapper) {
        return RecordConsumerSupport.startBuildingProcessor(mapper);
    }

    // Queries / accessors

    /**
     * Returns whether this consumer has encountered an asynchronous failure.
     *
     * @return {@code true} if a worker thread has failed, {@code false} otherwise
     */
    boolean hasFailedAsynchronously();

    /**
     * Returns whether this consumer has been closed.
     *
     * @return {@code true} if {@link #close()} has been called, {@code false} otherwise
     */
    boolean isClosed();

    /**
     * Returns the number of worker threads used by this consumer.
     *
     * @return the thread count (defaults to 1)
     */
    default int numOfThreads() {
        return 1;
    }

    /**
     * Returns the ordering strategy, if any.
     *
     * @return an {@link Optional} containing the {@link OrderStrategy}, or empty if unordered
     */
    default Optional<OrderStrategy> ordering() {
        return Optional.empty();
    }

    /**
     * Returns whether this consumer uses parallel processing.
     *
     * @return {@code true} if the consumer has more than one worker thread, {@code false} otherwise
     */
    default boolean isParallel() {
        return numOfThreads() > 1;
    }

    /**
     * Returns the error handling strategy for this consumer.
     *
     * @return the {@link RecordErrorHandlingStrategy}
     */
    RecordErrorHandlingStrategy errorStrategy();

    /**
     * Returns the record processor used by this consumer.
     *
     * @return the {@link RecordProcessor}
     */
    RecordProcessor<K, V> recordProcessor();

    /**
     * Returns the offset service for this consumer.
     *
     * @return the {@link OffsetService}
     */
    OffsetService offsetService();

    /**
     * Returns the monitor for tracking consumer metrics.
     *
     * @return the {@link Monitor}
     */
    Monitor monitor();

    // Mutators

    /**
     * Consumes a batch of records.
     *
     * <p>Implementations process each record in the batch according to the configured {@link
     * RecordProcessor} and error handling strategy.
     *
     * @param batch the batch of records to be consumed
     * @throws KafkaException if a record fails processing and the error strategy is {@link
     *     RecordErrorHandlingStrategy#FORCE_UNSUBSCRIPTION}, or if an unrecoverable error occurs
     */
    void consumeBatch(RecordBatch<K, V> batch);

    /**
     * Signals the end of the catch-up phase and transitions to realtime processing.
     *
     * <p>For parallel implementations, this drains all ring buffers up to the point of the call and
     * re-submits workers in realtime mode. For single-threaded implementations, subsequent records
     * are delivered as realtime events.
     *
     * @throws IllegalStateException if catch-up was not enabled at construction time
     */
    default void endCatchUp() {}

    /** Releases resources held by this consumer. */
    default void close() {}
}
