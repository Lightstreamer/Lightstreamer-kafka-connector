
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

package com.lightstreamer.kafka.connect.proxy;

public final class ProxyAdapterClientOptions {

    public static final class Builder {

        private String hostname;
        private int timeout;
        private int port;
        private int connectionMaxRetries;
        private long connectionRetryDelayMs;
        private String username;
        private String password;

        public Builder(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        public Builder connectionTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder connectionMaxRetries(int connectionMaxRetries) {
            this.connectionMaxRetries = connectionMaxRetries;
            return this;
        }

        public Builder connectionRetryDelayMs(long connectionRetryDelayMs) {
            this.connectionRetryDelayMs = connectionRetryDelayMs;
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

        public ProxyAdapterClientOptions build() {
            return new ProxyAdapterClientOptions(this);
        }
    }

    public final String hostname;
    public final int connectionTimeout;
    public final int port;
    public final int connectionMaxRetries;
    public final long connectionRetryDelayMs;
    public final String username;
    public final String password;

    private ProxyAdapterClientOptions(Builder builder) {
        this.hostname = builder.hostname;
        this.port = builder.port;
        this.connectionTimeout = builder.timeout;
        this.connectionMaxRetries = builder.connectionMaxRetries;
        this.connectionRetryDelayMs = builder.connectionRetryDelayMs;
        this.username = builder.username;
        this.password = builder.password;
    }
}
