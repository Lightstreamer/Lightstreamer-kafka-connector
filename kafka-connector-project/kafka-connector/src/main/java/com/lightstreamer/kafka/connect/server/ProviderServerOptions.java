
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

package com.lightstreamer.kafka.connect.server;

public final class ProviderServerOptions {

    public static final class Builder {

        private int port;
        private int maxProxyAdapterConnections;
        private String username;
        private String password;

        public Builder(int port) {
            this.port = port;
        }

        public Builder maxProxyAdapterConnections(int maxProxyAdapterConnections) {
            this.maxProxyAdapterConnections = maxProxyAdapterConnections;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public ProviderServerOptions build() {
            return new ProviderServerOptions(this);
        }
    }

    public final int port;
    public final String username;
    public final String password;
    public int maxProxyAdapterConnections;

    private ProviderServerOptions(Builder builder) {
        this.port = builder.port;
        this.maxProxyAdapterConnections = builder.maxProxyAdapterConnections;
        this.username = builder.username;
        this.password = builder.password;
    }
}
