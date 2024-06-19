
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

package com.lightstreamer.kafka.adapters.commons;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Enumeration;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class NonNullKeyProperties {

    private Properties properties;

    public NonNullKeyProperties() {
        this.properties = new Properties();
    }

    public void setProperty(String key, String value) {
        if (value != null) {
            properties.setProperty(key, value);
        }
    }

    public void putAll(Map<?, ?> t) {
        properties.putAll(t);
    }

    public Properties properties() {
        return new ImmutableProperties(properties);
    }

    private static class ImmutableProperties extends Properties {

        private final Properties props;

        ImmutableProperties(Properties props) {
            this.props = props;
        }

        @Override
        public String getProperty(String key) {
            return props.getProperty(key);
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            return props.getProperty(key, defaultValue);
        }

        public Enumeration<?> propertyNames() {
            return props.propertyNames();
        }

        public Set<String> stringPropertyNames() {

            return props.stringPropertyNames();
        }

        public void list(PrintStream out) {
            props.list(out);
        }

        public void list(PrintWriter out) {
            props.list(out);
        }

        @Override
        public int size() {
            return props.size();
        }

        @Override
        public Enumeration<Object> keys() {
            return props.keys();
        }

        @Override
        public Enumeration<Object> elements() {
            return props.elements();
        }

        @Override
        public boolean contains(Object value) {
            return props.contains(value);
        }

        @Override
        public boolean containsValue(Object value) {
            return props.containsValue(value);
        }

        @Override
        public boolean containsKey(Object key) {
            return props.containsKey(key);
        }

        @Override
        public synchronized Object remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void putAll(Map<?, ?> t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized String toString() {
            return props.toString();
        }

        @Override
        public void store(Writer writer, String comments) throws IOException {
            props.store(writer, comments);
        }

        @Override
        public void store(OutputStream out, String comments) throws IOException {
            props.store(out, comments);
        }

        @Override
        public void storeToXML(OutputStream os, String comment) throws IOException {
            props.storeToXML(os, comment);
        }

        @Override
        public void storeToXML(OutputStream os, String comment, String encoding)
                throws IOException {
            props.storeToXML(os, comment, encoding);
        }

        @Override
        public void storeToXML(OutputStream os, String comment, Charset charset)
                throws IOException {
            props.storeToXML(os, comment, comment);
        }

        @Override
        public Set<Object> keySet() {
            return props.keySet();
        }

        @Override
        public Collection<Object> values() {
            return props.values();
        }

        @Override
        public Set<Map.Entry<Object, Object>> entrySet() {
            return props.entrySet();
        }

        @Override
        public synchronized void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized Object put(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized Object setProperty(String key, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void load(Reader reader) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized void load(InputStream inStream) throws IOException {
            throw new UnsupportedOperationException();
        }

        public synchronized void loadFromXML(InputStream in)
                throws IOException, InvalidPropertiesFormatException {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized boolean equals(Object o) {
            return props.equals(o);
        }

        @Override
        public synchronized int hashCode() {
            return props.hashCode();
        }

        @Override
        public Object getOrDefault(Object key, Object defaultValue) {
            return props.getOrDefault(key, defaultValue);
        }

        @Override
        public synchronized void forEach(BiConsumer<? super Object, ? super Object> action) {
            props.forEach(action);
        }

        @Override
        public Object get(Object key) {
            return props.get(key);
        }

        @Override
        public synchronized void replaceAll(
                BiFunction<? super Object, ? super Object, ?> function) {
            props.replaceAll(function);
        }

        @Override
        public synchronized Object putIfAbsent(Object key, Object value) {
            return props.putIfAbsent(key, value);
        }

        @Override
        public synchronized boolean remove(Object key, Object value) {
            return props.remove(key, value);
        }

        @Override
        public synchronized boolean replace(Object key, Object oldValue, Object newValue) {
            return props.replace(key, oldValue, newValue);
        }

        @Override
        public synchronized Object replace(Object key, Object value) {
            return props.replace(key, value);
        }

        @Override
        public synchronized Object computeIfAbsent(
                Object key, Function<? super Object, ?> mappingFunction) {
            return props.computeIfAbsent(key, mappingFunction);
        }

        @Override
        public synchronized Object computeIfPresent(
                Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
            return props.computeIfPresent(key, remappingFunction);
        }

        @Override
        public synchronized Object compute(
                Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
            return props.compute(key, remappingFunction);
        }

        @Override
        public synchronized Object merge(
                Object key,
                Object value,
                BiFunction<? super Object, ? super Object, ?> remappingFunction) {
            return props.merge(key, value, remappingFunction);
        }

        @Override
        public synchronized Object clone() {
            return props.clone();
        }
    }
}
