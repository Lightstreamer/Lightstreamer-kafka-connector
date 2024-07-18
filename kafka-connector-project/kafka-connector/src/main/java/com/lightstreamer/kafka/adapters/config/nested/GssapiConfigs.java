
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

package com.lightstreamer.kafka.adapters.config.nested;

import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.BOOL;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.DefaultHolder.defaultValue;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;

public class GssapiConfigs {

    public static final String GSSAPI_KERBEROS_SERVICE_NAME = "gssapi.kerberos.service.name";
    public static final String GSSAPI_KEY_TAB_ENABLE = "gssapi.key.tab.enable";
    public static final String GSSAPI_KEY_TAB_PATH = "gssapi.key.tab.path";
    public static final String GSSAPI_STORE_KEY_ENABLE = "gssapi.store.key.enable";
    public static final String GSSAPI_PRINCIPAL = "gssapi.principal";
    public static final String GSSAPI_TICKET_CACHE_ENABLE = "gssapi.ticket.cache.enable";

    private static ConfigsSpec GSSAPI_CONFIG_SEPC;

    static {
        GSSAPI_CONFIG_SEPC =
                new ConfigsSpec("gssapi")
                        .add(GSSAPI_KERBEROS_SERVICE_NAME, true, false, TEXT)
                        .add(GSSAPI_KEY_TAB_ENABLE, false, false, BOOL, defaultValue("false"))
                        .add(GSSAPI_KEY_TAB_PATH, false, false, FILE)
                        .add(GSSAPI_STORE_KEY_ENABLE, false, false, BOOL, defaultValue("false"))
                        .add(GSSAPI_PRINCIPAL, false, false, TEXT)
                        .add(GSSAPI_TICKET_CACHE_ENABLE, false, false, BOOL, defaultValue("false"));
    }

    public static ConfigsSpec spec() {
        return GSSAPI_CONFIG_SEPC;
    }

    private GssapiConfigs() {}
}
