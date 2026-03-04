
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

package com.lightstreamer.kafka.adapters.config;

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.FILE;
import static com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec.ConfType.TEXT;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lightstreamer.kafka.adapters.config.specs.ConfigsSpec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AbstractConfigTest {

    private Path adapterDir;
    private Path schemaFile1;
    private Path schemaFile2;
    private Path keystoreFile;

    @BeforeEach
    public void setUp() throws IOException {
        adapterDir = Files.createTempDirectory("adapter_dir_test");

        // Create subdirectory for nested paths
        Path schemasDir = Files.createDirectory(adapterDir.resolve("schemas"));
        Path certsDir = Files.createDirectory(adapterDir.resolve("certs"));

        // Create test files
        schemaFile1 = Files.createTempFile(schemasDir, "schema1-", ".avsc");
        schemaFile2 = Files.createTempFile(schemasDir, "schema2-", ".avsc");
        keystoreFile = Files.createTempFile(certsDir, "keystore-", ".jks");
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (schemaFile1 != null) Files.deleteIfExists(schemaFile1);
        if (schemaFile2 != null) Files.deleteIfExists(schemaFile2);
        if (keystoreFile != null) Files.deleteIfExists(keystoreFile);

        if (adapterDir != null) {
            Files.walk(adapterDir)
                    .sorted((a, b) -> -a.compareTo(b)) // Delete files before directories
                    .forEach(
                            path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException e) {
                                    // Ignore
                                }
                            });
        }
    }

    @Test
    public void shouldPreserveConfigWithoutFileParameters() {
        ConfigsSpec spec =
                new ConfigsSpec()
                        .add("adapters_conf.id", true, false, TEXT)
                        .add("param1", false, false, TEXT);

        Map<String, String> config = new HashMap<>();
        config.put("adapters_conf.id", "KAFKA");
        config.put("param1", "value1");
        config.put("custom.entry", "/some/other/path");

        Map<String, String> result =
                AbstractConfig.resolveFilePaths(spec, config, adapterDir.toFile());

        // All entries should be preserved unchanged when no FILE parameters processed
        assertThat(result).containsEntry("adapters_conf.id", "KAFKA");
        assertThat(result).containsEntry("param1", "value1");
        assertThat(result).containsEntry("custom.entry", "/some/other/path");
        assertThat(result).hasSize(3);
    }

    @Test
    public void shouldOnlyConvertFileTypeParameters() {
        String relativeSchemaPath1 = "schemas/" + schemaFile1.getFileName().toString();
        String relativeSchemaPath2 = "schemas/" + schemaFile2.getFileName().toString();
        String relativeKeystorePath = "certs/" + keystoreFile.getFileName().toString();

        ConfigsSpec spec =
                new ConfigsSpec()
                        .add("schema.key.path", false, false, FILE)
                        .add("schema.value.path", false, false, FILE)
                        .add("keystore.path", false, false, FILE)
                        .add("parent.path", false, false, FILE)
                        .add("current.path", false, false, FILE)
                        .add("text.param", false, false, TEXT);

        Map<String, String> config = new HashMap<>();
        config.put("schema.key.path", relativeSchemaPath1);
        config.put("schema.value.path", relativeSchemaPath2);
        config.put("keystore.path", relativeKeystorePath);
        config.put("parent.path", "../sibling/schema.avsc");
        config.put("current.path", "./schemas/schema.avsc");
        config.put("text.param", "some-text-value");

        int originalSize = config.size();

        Map<String, String> result =
                AbstractConfig.resolveFilePaths(spec, config, adapterDir.toFile());

        // FILE parameters should be converted to absolute
        assertThat(result)
                .containsEntry(
                        "schema.key.path", adapterDir.resolve(relativeSchemaPath1).toString());
        assertThat(result)
                .containsEntry(
                        "schema.value.path", adapterDir.resolve(relativeSchemaPath2).toString());
        assertThat(result)
                .containsEntry(
                        "keystore.path", adapterDir.resolve(relativeKeystorePath).toString());
        assertThat(result)
                .containsEntry(
                        "parent.path",
                        Paths.get(adapterDir.toString(), "../sibling/schema.avsc").toString());
        assertThat(result)
                .containsEntry(
                        "current.path",
                        Paths.get(adapterDir.toString(), "./schemas/schema.avsc").toString());

        // TEXT parameter should remain unchanged
        assertThat(result).containsEntry("text.param", "some-text-value");

        // Original map should remain unchanged
        assertThat(config).containsEntry("schema.key.path", relativeSchemaPath1);
        assertThat(config).containsEntry("schema.value.path", relativeSchemaPath2);
        assertThat(config).containsEntry("keystore.path", relativeKeystorePath);
        assertThat(config).containsEntry("parent.path", "../sibling/schema.avsc");
        assertThat(config).containsEntry("current.path", "./schemas/schema.avsc");
        assertThat(config).containsEntry("text.param", "some-text-value");
        assertThat(config).hasSize(originalSize);
    }

    @ParameterizedTest
    @CsvSource({"/absolute/path/to/schema.avsc", "C:\\absolute\\path\\to\\schema.avsc"})
    public void shouldPreserveAbsoluteFilePaths(String absolutePath) {
        ConfigsSpec spec = new ConfigsSpec().add("schema.path", false, false, FILE);

        Map<String, String> config = new HashMap<>();
        config.put("schema.path", absolutePath);

        Map<String, String> result =
                AbstractConfig.resolveFilePaths(spec, config, adapterDir.toFile());

        // Absolute paths should remain unchanged (only test if path is absolute on this OS)
        if (Paths.get(absolutePath).isAbsolute()) {
            assertThat(result).containsEntry("schema.path", absolutePath);
        }
    }

    @Test
    public void shouldSkipInvalidFileParameterValues() {
        ConfigsSpec spec =
                new ConfigsSpec()
                        .add("schema.path", false, false, FILE)
                        .add("keystore.path", false, false, FILE)
                        .add("truststore.path", false, false, FILE);

        Map<String, String> config = new HashMap<>();
        config.put("schema.path", null);
        config.put("keystore.path", "");
        config.put("truststore.path", "   ");

        Map<String, String> result =
                AbstractConfig.resolveFilePaths(spec, config, adapterDir.toFile());

        // Null, empty, and whitespace-only values should be preserved (not converted)
        assertThat(result).containsEntry("schema.path", null);
        assertThat(result).containsEntry("keystore.path", "");
        assertThat(result).containsEntry("truststore.path", "   ");
    }

    @Test
    public void shouldThrowNullPointerExceptionForNullConfigDir() {
        ConfigsSpec spec = new ConfigsSpec().add("param1", false, false, TEXT);
        Map<String, String> config = Collections.emptyMap();

        assertThrows(
                NullPointerException.class,
                () -> {
                    AbstractConfig.resolveFilePaths(spec, config, null);
                });
    }
}
