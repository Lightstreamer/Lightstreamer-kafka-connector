
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

public interface ConstantSelector extends Selector {

    // default Value extract(Constant constant, KafkaRecord<?, ?> record) throws ValueException {
    //     Object value =
    //             switch (constant) {
    //                 case TIMESTAMP -> String.valueOf(record.timestamp());
    //                 case PARTITION -> String.valueOf(record.partition());
    //                 case OFFSET -> String.valueOf(record.offset());
    //                 case TOPIC -> record.topic();
    //                 case KEY -> record.key();
    //                 case VALUE -> record.value();
    //             };
    //     return new SimpleValue(name(), Objects.toString(value, null));
    // }

    Data extract(KafkaRecord<?, ?> record) throws ValueException;
}
