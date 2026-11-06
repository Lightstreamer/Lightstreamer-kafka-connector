
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

package com.lightstreamer.kafka.common.config;

import com.lightstreamer.kafka.common.utils.Split;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DestinationMappingConfig {

    private final String destination;
    private final Set<String> topics;

    private DestinationMappingConfig(String destination, Set<String> topics) {
        this.destination = checkAndTrimDestination(destination);
        this.topics = checkAndTrimMappings(topics);
    }

    private String checkAndTrimDestination(String destination) {
        if (destination == null || destination.isBlank()) {
            throw new ConfigException("Destination must be a non-empty string");
        }
        return destination.trim();
    }

    private Set<String> checkAndTrimMappings(Set<String> mappings) throws ConfigException {
        LinkedHashSet<String> trimmed = new LinkedHashSet<>();
        for (String mapping : mappings) {
            if (mapping.isBlank()) {
                throw new ConfigException("Destination mappings must be non-empty strings");
            }
            trimmed.add(mapping.trim());
        }
        return Collections.unmodifiableSet(trimmed);
    }

    public String destination() {
        return destination;
    }

    public Set<String> topics() {
        return topics;
    }

    public static DestinationMappingConfig fromDelimitedMappings(
            String destination, String delimitedMapping) {
        return new DestinationMappingConfig(
                destination, new LinkedHashSet<>(Split.byComma(delimitedMapping)));
    }

    public static List<DestinationMappingConfig> from(Map<String, String> destinationToTopics)
            throws ConfigException {
        List<DestinationMappingConfig> configs = new ArrayList<>();
        for (Map.Entry<String, String> entry : destinationToTopics.entrySet()) {
            String destination = entry.getKey();
            String mappings = entry.getValue();
            configs.add(fromDelimitedMappings(destination, mappings));
        }
        return configs;
    }
}
