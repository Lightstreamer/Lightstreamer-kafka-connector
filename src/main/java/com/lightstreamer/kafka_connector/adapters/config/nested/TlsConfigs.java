
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

import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigTypes.SslProtocol;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;
import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec.ConfType;

public class TlsConfigs {

    public static String SSL_ENABLED_PROTOCOLS = "enabled.protocols";
    public static String SSL_PROTOCOL = "protocol";

    public static String TRUSTSTORE_TYPE = "truststore.type";
    public static String TRUSTSTORE_PATH = "truststore.path";
    public static String TRUSTSTORE_PASSWORD = "truststore.password";

    public static String ENABLE_KESYTORE = "keystore.enabled";

    public static String ENABLE_HOSTNAME_VERIFICATION = "hostname.verification";
    public static String SSL_CIPHER_SUITES = "cipher.suites";
    public static String SSL_PROVIDER = "provider";
    public static String SSL_EGINE_FACTORY_CLASS = "engine.factory.class";
    public static String SSL_KEYMANAGER_ALGORITHM = "keymanager.algorithm";
    public static String SSL_SECURE_RANDOM_IMPLEMENTATION = "secure.random.implementation";
    public static String SSL_TRUSTMANAGER_ALGORITHM = "trustmanager.algorithm";
    public static String SECURITY_PROVIDERS = "security.providers";

    private static ConfigsSpec CONFIG_SPEC;

    static {
        CONFIG_SPEC =
                new ConfigsSpec("TLS")
                        .add(
                                SSL_ENABLED_PROTOCOLS,
                                false,
                                false,
                                ConfType.SSL_ENABLED_PROTOCOLS,
                                defaultValue(ConfigTypes.SslProtocol.toValuesStr()))
                        .add(
                                SSL_PROTOCOL,
                                false,
                                false,
                                ConfType.SSL_PROTOCOL,
                                defaultValue(SslProtocol.TLSv13.toString()))
                        .add(
                                TRUSTSTORE_TYPE,
                                false,
                                false,
                                ConfType.KEYSTORE_TYPE,
                                defaultValue(KeystoreType.JKS.toString()))
                        .add(TRUSTSTORE_PATH, false, false, FILE)
                        .add(TRUSTSTORE_PASSWORD, false, false, TEXT)
                        .add(
                                ENABLE_HOSTNAME_VERIFICATION,
                                false,
                                false,
                                BOOL,
                                defaultValue("false"))
                        .add(SSL_CIPHER_SUITES, false, false, ConfType.TEXT_LIST)
                        .add(SSL_PROVIDER, false, false, TEXT)
                        .add(SSL_EGINE_FACTORY_CLASS, false, false, TEXT)
                        .add(SSL_KEYMANAGER_ALGORITHM, false, false, TEXT)
                        .add(SSL_SECURE_RANDOM_IMPLEMENTATION, false, false, TEXT)
                        .add(SSL_TRUSTMANAGER_ALGORITHM, false, false, TEXT)
                        .add(SECURITY_PROVIDERS, false, false, TEXT)
                        .add(ENABLE_KESYTORE, false, false, ConfType.BOOL, defaultValue("false"))
                        .withEnabledChildConfigs(KeystoreConfigs.spec(), ENABLE_KESYTORE);
    }

    public static ConfigsSpec spec() {
        return CONFIG_SPEC;
    }
}
