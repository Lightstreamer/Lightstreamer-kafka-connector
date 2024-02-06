
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

package com.lightstreamer.kafka_connector.adapters;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.lightstreamer.kafka_connector.adapters.test_utils.ConnectorConfigProvider;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConnectorDataAdapterTest {

    private File adapterDir;
    private ConnectorDataAdapter connectorDataAdapter;

    @BeforeEach
    public void before() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir").toFile();
        connectorDataAdapter = new ConnectorDataAdapter();
    }

    @Test
    void shouldInit() {
        Map<String, String> params = ConnectorConfigProvider.minimalConfigParams();
        assertDoesNotThrow(() -> connectorDataAdapter.init(params, adapterDir));
    }
}
