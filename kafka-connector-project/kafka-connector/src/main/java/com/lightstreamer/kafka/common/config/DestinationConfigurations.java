
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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class DestinationConfigurations {

    public static class DestinationConfig {

        private final String destination;
        private final Set<String> topics;

        private DestinationConfig(String destination, Set<String> topics) {
            this.destination = checkAndTrimDestination(destination);
            this.topics = checkAndTrimMappings(topics);
        }

        private static String checkAndTrimDestination(String destination) {
            if (destination == null || destination.isBlank()) {
                throw new ConfigException("Destination must be a non-empty string");
            }
            return destination.trim();
        }

        private static Set<String> checkAndTrimMappings(Set<String> mappings)
                throws ConfigException {
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
    }

    public static DestinationConfigurations from(Map<String, String> destinationToTopics)
            throws ConfigException {
        Map<String, DestinationConfig> configs = new HashMap<>();
        for (Map.Entry<String, String> entry : destinationToTopics.entrySet()) {
            String destination = entry.getKey();
            String mappings = entry.getValue();
            configs.put(
                    destination,
                    new DestinationConfig(
                            destination, new LinkedHashSet<>(Split.byComma(mappings))));
        }
        return new DestinationConfigurations(configs);
    }

    private final Map<String, DestinationConfig> configs;

    DestinationConfigurations(Map<String, DestinationConfig> configs) {
        this.configs = configs;
    }

    public Set<String> lookupTopics(String destination) {
        DestinationConfig config = configs.get(destination);
        if (config == null) {
            return Collections.emptySet();
        }
        return config.topics();
    }

    public int size() {
        return configs.size();
    }
}
