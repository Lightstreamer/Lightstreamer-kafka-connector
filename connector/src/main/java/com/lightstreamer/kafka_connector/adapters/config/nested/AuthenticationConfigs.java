
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

package com.lightstreamer.kafka_connector.adapters.config.nested;

import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType;

public class AuthenticationConfigs {

    public static final String SASL_MECHANISM = "mechanism";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    private static ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("authentication")
                        .add(
                                SASL_MECHANISM,
                                false,
                                false,
                                ConfType.SASL_MECHANISM,
                                defaultValue(SaslMechanism.PLAIN.toString()))
                        .add(USERNAME, false, false, TEXT)
                        .add(PASSWORD, false, false, TEXT);
    }

    public static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    public static ConfigsSpec cloneSpec() {
        return new ConfigsSpec(CONFIG_SPEC);
    }
}
