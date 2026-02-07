
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

package com.lightstreamer.kafka.common.mapping.monitors;

import com.lightstreamer.kafka.common.records.RecordBatch;

/**
 * Interface for monitoring batch processing and buffer utilization.
 *
 * <p>Implementations can track processing throughput, batch completion events, and buffer
 * utilization to provide observability into the record processing pipeline.
 *
 * <p>This interface combines {@link RecordBatch.RecordBatchListener} for event-driven batch
 * completion tracking with {@link Monitor} for periodic statistics reporting.
 *
 * <p><b>Thread Safety:</b> Implementations must be thread-safe as methods may be called
 * concurrently from multiple worker threads.
 *
 * @see ThroughputMonitor
 */
public interface ProcessingMonitor extends RecordBatch.RecordBatchListener, Monitor {}
