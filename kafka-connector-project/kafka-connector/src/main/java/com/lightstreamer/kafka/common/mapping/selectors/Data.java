
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

package com.lightstreamer.kafka.common.mapping.selectors;

public interface Data extends Comparable<Data> {

    String name();

    String text();

    @Override
    public default int compareTo(Data o) {
        return this.name().compareTo(o.name());
    }

    static Data from(String name, String text) {
        return new SimpleData(name, text);
    }

    static String buildItemNameSingle(Data data, String schemaName) {
        return schemaName + "-[" + data.name() + "=" + data.text() + "]";
    }

    static String buildItemName(Data[] dataList, String schemaName) {
        if (dataList.length == 0) {
            return schemaName;
        }

        if (dataList.length == 1) {
            Data data = dataList[0];
            // Still create temp strings for single case - acceptable overhead
            return schemaName + "-[" + data.name() + "=" + data.text() + "]";
        }

        // Calculate exact size needed - no resizing
        int totalSize = schemaName.length() + 3; // schemaName + "-[" + "]"
        for (Data data : dataList) {
            totalSize +=
                    data.name().length() + 1 + data.text().length() + 1; // name + "=" + text + ","
        }
        totalSize--; // Remove last comma

        char[] chars = new char[totalSize];
        int pos = 0;

        // Direct memory copy - fastest possible
        schemaName.getChars(0, schemaName.length(), chars, pos);
        pos += schemaName.length();

        chars[pos++] = '-';
        chars[pos++] = '[';

        // Direct char array writes - no StringBuilder overhead
        for (int i = 0; i < dataList.length; i++) {
            if (i > 0) {
                chars[pos++] = ',';
            }

            Data data = dataList[i];
            String name = data.name();
            String text = data.text();

            // Direct memory operations
            name.getChars(0, name.length(), chars, pos);
            pos += name.length();
            chars[pos++] = '=';
            text.getChars(0, text.length(), chars, pos);
            pos += text.length();
        }

        chars[pos] = ']';
        return new String(chars);
    }
}

record SimpleData(String name, String text) implements Data {}
