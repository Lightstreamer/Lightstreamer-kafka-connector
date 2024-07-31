
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

package com.lightstreamer.kafka.common.utils;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka.test_utils.VersionUtils;

public class VersionTest {

    static String VERSION;

    @BeforeAll
    public static void before() {
        VERSION = VersionUtils.currentVersion();
    }

    @Test
    void shouldGetExpectedVersion() {
        String version = Version.getVersion();
        assertThat(version).isEqualTo(VERSION);
    }

    @Test
    void shouldGetUnknownVersion() {
        String version = Version.getVersion("non-existing-version.properties");
        assertThat(version).isEqualTo("unknown");
    }
}
