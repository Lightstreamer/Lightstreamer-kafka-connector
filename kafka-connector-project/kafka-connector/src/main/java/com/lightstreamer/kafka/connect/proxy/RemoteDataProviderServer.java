
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

import com.lightstreamer.adapters.remote.DataProvider;
import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.DataProviderServer;
import com.lightstreamer.adapters.remote.ExceptionHandler;
import com.lightstreamer.adapters.remote.MetadataProviderException;
import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.log.LogManager;
import com.lightstreamer.log.LoggerProvider;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;

public interface RemoteDataProviderServer {

    record IOStreams(InputStream in, OutputStream out) {}

    void setCredentials(String username, String password);

    void setIOStreams(IOStreams streams);

    void setAdapter(DataProvider provider);

    void start() throws ConnectException;

    void close();

    void setExceptionHandler(ExceptionHandler handler);

    static RemoteDataProviderServer newDataProviderServer() {
        return new RemoteDataProviderServerImpl();
    }
}

class RemoteDataProviderServerImpl implements RemoteDataProviderServer {

    private static final Logger logger = LoggerFactory.getLogger(RemoteDataProviderServer.class);

    static {
        LogManager.setLoggerProvider(
                new LoggerProvider() {

                    @Override
                    public com.lightstreamer.log.Logger getLogger(String category) {
                        return new LoggerWrapper(logger);
                    }
                });
    }

    private final DataProviderServer server;

    RemoteDataProviderServerImpl() {
        this.server = new DataProviderServer();
    }

    @Override
    public void setCredentials(String username, String password) {
        server.setRemoteUser(username);
        server.setRemotePassword(password);
    }

    @Override
    public void setIOStreams(IOStreams streams) {
        server.setRequestStream(streams.in());
        server.setReplyStream(streams.out());
    }

    @Override
    public void setAdapter(DataProvider provider) {
        server.setAdapter(provider);
    }

    @Override
    public void setExceptionHandler(ExceptionHandler handler) {
        server.setExceptionHandler(handler);
    }

    @Override
    public void start() {
        try {
            server.start();
        } catch (MetadataProviderException | DataProviderException e) {
            // No longer thrown
        } catch (RemotingException re) {
            throw new ConnectException(re);
        }
    }

    @Override
    public void close() {
        server.close();
    }
}
