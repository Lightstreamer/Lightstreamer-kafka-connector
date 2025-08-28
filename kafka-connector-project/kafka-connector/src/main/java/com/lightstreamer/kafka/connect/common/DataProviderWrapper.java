
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

package com.lightstreamer.kafka.connect.common;

import com.lightstreamer.adapters.remote.DataProviderException;
import com.lightstreamer.adapters.remote.DataProviderServer;
import com.lightstreamer.adapters.remote.ExceptionHandler;
import com.lightstreamer.adapters.remote.MetadataProviderException;
import com.lightstreamer.adapters.remote.RemotingException;
import com.lightstreamer.log.LogManager;
import com.lightstreamer.log.LoggerProvider;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;

public interface DataProviderWrapper {

    @FunctionalInterface
    interface CloseHook {

        void closed(DataProviderWrapper provider);
    }

    record IOStreams(InputStream in, OutputStream out) {
        public IOStreams(Socket socket) throws IOException {
            this(socket.getInputStream(), socket.getOutputStream());
        }
    }

    void setCredentials(String username, String password);

    void setIOStreams(IOStreams streams);

    void setExceptionHandler(ExceptionHandler handler);

    void setCloseHook(CloseHook hook);

    void start() throws ConnectException;

    void sendRecords(Collection<SinkRecord> records);

    Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets);

    void close();

    static DataProviderWrapper newWrapper(RecordSender sender) {
        return new DataProviderWrapperImpl(sender);
    }
}

class DataProviderWrapperImpl implements DataProviderWrapper {

    private static final Logger logger = LoggerFactory.getLogger(DataProviderWrapper.class);

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
    private final RecordSender sender;
    private CloseHook hook;

    DataProviderWrapperImpl(RecordSender sender) {
        this.server = new DataProviderServer();
        this.server.setAdapter(sender);
        this.sender = sender;
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
    public void setCloseHook(CloseHook hook) {
        this.hook = hook;
    }

    @Override
    public void sendRecords(Collection<SinkRecord> records) {
        sender.sendRecords(records);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        return sender.preCommit(offsets);
    }

    @Override
    public void close() {
        server.close();
        if (hook != null) {
            hook.closed(this);
        }
    }
}

class LoggerWrapper implements com.lightstreamer.log.Logger {

    private final Logger logger;

    public LoggerWrapper(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void error(String line) {
        logger.error(line);
    }

    @Override
    public void error(String line, Throwable exception) {
        logger.error(line, exception);
    }

    @Override
    public void warn(String line) {
        logger.warn(line);
    }

    @Override
    public void warn(String line, Throwable exception) {
        logger.warn(line, exception);
    }

    @Override
    public void info(String line) {
        logger.info(line);
    }

    @Override
    public void info(String line, Throwable exception) {
        logger.info(line, exception);
    }

    @Override
    public void debug(String line) {
        logger.debug(line);
    }

    @Override
    public void debug(String line, Throwable exception) {
        logger.debug(line, exception);
    }

    @Override
    public void fatal(String line) {
        error(line);
    }

    @Override
    public void fatal(String line, Throwable exception) {
        error(line, exception);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isFatalEnabled() {
        return logger.isErrorEnabled();
    }
}
