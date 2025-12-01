
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

public interface GenericSelector extends Selector {

    default Data extract(String name, KafkaRecord<?, ?> record) throws ValueException {
        return extract(name, record, true);
    }

    Data extract(String name, KafkaRecord<?, ?> record, boolean checkScalar) throws ValueException;

    default Data extract(KafkaRecord<?, ?> record) throws ValueException {
        return extract(record, true);
    }

    Data extract(KafkaRecord<?, ?> record, boolean checkScalar) throws ValueException;

    void extractInto(KafkaRecord<?, ?> record, Map<String, String> target) throws ValueException;
}
