
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

package com.lightstreamer.kafka.common.mapping.selectors;

import com.lightstreamer.kafka.common.records.KafkaRecord;

import java.util.Map;

public interface KeySelector<K> extends Selector {

    default Data extractKey(String name, KafkaRecord<K, ?> record) throws ValueException {
        return extractKey(name, record, true);
    }

    Data extractKey(String name, KafkaRecord<K, ?> record, boolean checkScalar)
            throws ValueException;

    default Data extractKey(KafkaRecord<K, ?> record) throws ValueException {
        return extractKey(record, true);
    }

    Data extractKey(KafkaRecord<K, ?> record, boolean checkScalar) throws ValueException;

    void extractKeyInto(KafkaRecord<K, ?> record, Map<String, String> target) throws ValueException;
}
