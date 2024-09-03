
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

import com.lightstreamer.adapters.remote.DataProvider;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Map;

/**
 * Interface used by a {@code com.lightstreamer.kafka.connect.proxy.ProxyAdapterClient } to forward
 * the records received from Kafka Connect to the target Lightstreamer Server instance.
 */
public interface RecordSender extends DataProvider {

    /**
     * Sends the records to Lightstreamer Server.
     *
     * @param records the collection fo records to send
     */
    void sendRecords(Collection<SinkRecord> records);

    /**
     * Pre-commit hook invoked prior to an offset commit.
     *
     * <p>The signature of the method is identical to {@link
     * org.apache.kafka.connect.sink.SinkTask#preCommit(Map)}.
     *
     * @param currentOffsets the current offset state of the last call to {@link
     *     #sendRecords(Collection)}
     * @return a map of offsets by topic-partition that are safe to commit
     * @see org.apache.kafka.connect.sink.SinkTask#preCommit(Map)
     */
    Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets);
}
