
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

package com.lightstreamer.kafka_connector.adapters.config;

import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.commons.SkipNullKeyProperties;
import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.KeystoreType;

import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class KeystoreConfigs {

    public static String KEYSTORE_TYPE = "encryption.keystore.type";

    public static String KEYSTORE_PATH = "encryption.keystore.path";

    public static String KEYSTORE_PASSWORD = "encryption.keystore.password";

    public static String KEY_PASSWORD = "encryption.key.password";

    private static ConfigSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigSpec("keyStore")
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

    static ConfigSpec configSpec() {
        return CONFIG_SPEC;
    }

    static void withKeystoreConfigs(ConfigSpec config, String enablingKey) {
        config.addConfigSpec(CONFIG_SPEC, enablingKey);
    }

    static Properties addKeystore(ConnectorConfig config) {
        SkipNullKeyProperties properties = new SkipNullKeyProperties();
        if (config.isKeystoreEnabled()) {
            properties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, config.getKeystoreType());
            properties.setProperty(
                    SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.getKeystorePassword());
            properties.setProperty(
                    SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.getKeystorePath());
            properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, config.getKeyPassword());
        }

        return properties.properties();
    }
}
