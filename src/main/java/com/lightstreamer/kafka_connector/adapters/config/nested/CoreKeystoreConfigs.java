
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

import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType;

public class CoreKeystoreConfigs {

    public static String KEYSTORE_TYPE = "keystore.type";

    public static String KEYSTORE_PATH = "keystore.path";

    public static String KEYSTORE_PASSWORD = "keystore.password";

    public static String KEY_PASSWORD = "key.password";

    private static ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("KEYSTORE")
                        .add(
                                KEYSTORE_TYPE,
                                false,
                                false,
                                ConfType.KEYSTORE_TYPE,
                                defaultValue(KeystoreType.JKS.toString()))
                        .add(KEYSTORE_PATH, true, false, FILE)
                        .add(KEYSTORE_PASSWORD, true, false, TEXT)
                        .add(KEY_PASSWORD, false, false, TEXT);
    }

    static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }

    // static Properties addKeystore(ConnectorConfig config) {
    //     NoNullKeyProperties props = new NoNullKeyProperties();
    //     if (config.isKeystoreEnabled()) {
    //         props.setProperty(
    //                 SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, config.getKeystoreType().toString());
    //         props.setProperty(
    //                 SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.getKeystorePassword());
    //         props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.getKeystorePath());
    //         props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, config.getKeyPassword());
    //     }

    //     return props.properties();
    // }
}
