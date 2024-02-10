
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

import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.BOOL;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.FILE;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.KEYSTORE_TYPE;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.SECURITY_PROTOCOL;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.SSL_PROTOCOL;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.TEXT;
import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SecurityProtocol;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SslProtocol;

public class EncryptionConfigs {

    public static String ENBALE_ENCRYTPTION = "encryption.enable";

    public static String SECURITY_PROTOCOL = "encryption.security.protocol";
    public static String SECURITY_PROVIDERS = "encryption.security.providerS";

    public static String SSL_CIPHER_SUITES = "encryption.ssl.cipher.suites";
    public static String SSL_PROTOCOL = "encryption.ssl.protocol";
    public static String SSL_PROVIDER = "encryption.ssl.provider";
    public static String SSL_ENDPOINT_IDENTIFICATAION_ALGORITHM =
            "encryption.ssl.endpoint.identification.algorithm";
    public static String SSL_EGINE_FACTORY_CLASS = "encryption.ssl.engine.factory.class";
    public static String SSL_KEYMANAGER_ALGORITHM = "encryption.ssl.keymanager.algorithm";
    public static String SSL_SECURE_RANDOM_IMPLEMENTATION =
            "encryption.ssl.secure.random.implementation";
    public static String SSL_TRUSTMANAGER_ALGORITHM = "encryption.ssl.trustmanager.algorithm";
    public static String SSL_ENABLED_PROTOCOLS = "encryption.ssl.enabled.protocols";

    public static String SSL_TRUSTSTORE_TYPE = "encryption.ssl.truststore.type";
    public static String SSL_TRUSTSTORE_LOCATION = "encryption.ssl.truststore.file";
    public static String SSL_TRUSTSTORE_PASSWORD = "encryption.ssl.truststore.password";

    public static String SSL_KEYSTORE_TYPE = "encryption.ssl.keystore.type";
    public static String SSL_KEYSTORE_LOCATION = "encryption.ssl.keystore.file";
    public static String SSL_KEYSTORE_PASSWORD = "encryption.ssl.keystore.password";

    static void withEncryptionConfig(ConfigSpec configs) {
        configs.add(ENBALE_ENCRYTPTION, false, false, BOOL, defaultValue("false"))
                .add(SECURITY_PROVIDERS, false, false, TEXT)
                .add(SSL_ENDPOINT_IDENTIFICATAION_ALGORITHM, false, false, TEXT)
                .add(SSL_EGINE_FACTORY_CLASS, false, false, TEXT)
                .add(SSL_KEYMANAGER_ALGORITHM, false, false, TEXT)
                .add(SSL_SECURE_RANDOM_IMPLEMENTATION, false, false, TEXT)
                .add(SSL_TRUSTMANAGER_ALGORITHM, false, false, TEXT)
                .add(
                        SSL_CIPHER_SUITES,
                        false,
                        false,
                        ConfType.SECURITY_PROTOCOL,
                        defaultValue(SecurityProtocol.PLAIN_TEXT.toString()))
                .add(
                        SECURITY_PROTOCOL,
                        false,
                        false,
                        ConfType.SECURITY_PROTOCOL,
                        defaultValue(SecurityProtocol.PLAIN_TEXT.toString()))
                .add(SSL_PROVIDER, false, false, TEXT)
                .add(
                        SSL_PROTOCOL,
                        false,
                        false,
                        ConfType.SSL_PROTOCOL,
                        defaultValue(SslProtocol.TLSv13.toString()))
                .add(
                        SSL_TRUSTSTORE_TYPE,
                        false,
                        false,
                        KEYSTORE_TYPE,
                        defaultValue(KeystoreType.JKS.toString()))
                .add(SSL_TRUSTSTORE_LOCATION, false, false, FILE)
                .add(SSL_TRUSTSTORE_PASSWORD, false, false, TEXT)
                .add(
                        SSL_KEYSTORE_TYPE,
                        false,
                        false,
                        KEYSTORE_TYPE,
                        defaultValue(KeystoreType.JKS.toString()))
                .add(SSL_KEYSTORE_LOCATION, false, false, FILE)
                .add(SSL_KEYSTORE_PASSWORD, false, false, TEXT);
    }
}
