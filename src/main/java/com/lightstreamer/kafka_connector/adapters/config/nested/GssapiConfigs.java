
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

import com.lightstreamer.kafka_connector.adapters.config.specs.ConfigsSpec;

public class GssapiConfigs {

    public static final String GSSAPI_KERBEROS_SERVICE_NAME = "gssapi.kerberos.service.name";
    public static final String GSSAPI_USE_KEY_TAB = "gssapi.use.key.tab";
    public static final String GSSAPI_KEY_TAB = "gssapi.key.tab";
    public static final String GSSAPI_STORE_KEY = "gssapi.store.key";
    public static final String GSSAPI_PRINCIPAL = "gssapi.principal";
    public static final String GSSAPI_USE_TICKET_CACHE = "gssapi.use.ticket.cache";

    private static ConfigsSpec GSSAPI_CONFIG_SEPC;

    static {
        GSSAPI_CONFIG_SEPC =
                new ConfigsSpec("gssapi")
                        .add(GSSAPI_KERBEROS_SERVICE_NAME, true, false, TEXT)
                        .add(GSSAPI_USE_KEY_TAB, false, false, BOOL, defaultValue("false"))
                        .add(GSSAPI_KEY_TAB, false, false, FILE)
                        .add(GSSAPI_STORE_KEY, false, false, BOOL, defaultValue("false"))
                        .add(GSSAPI_PRINCIPAL, false, false, TEXT)
                        .add(GSSAPI_USE_TICKET_CACHE, false, false, BOOL, defaultValue("false"));
    }

    public static ConfigsSpec spec() {
        return GSSAPI_CONFIG_SEPC;
    }
}
