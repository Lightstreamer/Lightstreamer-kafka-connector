
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.common.mapping.selectors;

import java.util.Map;
import java.util.Set;

public interface FieldsExtractor<K, V> {

    default Map<String, String> extractMap(KafkaRecord<K, V> record) throws ValueException {
        Map<String, String> values = new java.util.HashMap<>();
        extractIntoMap(record, values);
        return values;
    }

    void extractIntoMap(KafkaRecord<K, V> record, Map<String, String> targetMap)
            throws ValueException;

    boolean skipOnFailure();

    boolean mapNonScalars();

    Set<String> mappedFields();
}
