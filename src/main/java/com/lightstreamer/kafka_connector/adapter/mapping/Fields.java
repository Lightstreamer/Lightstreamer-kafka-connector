package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.Builder;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors.SelectorsSupplier;

public class Fields {

    public interface FieldMappings<K, V> {
        Selectors<K, V> selectors();
    }

    public static <K, V> FieldMappings<K, V> fieldMappingsFrom(Map<String, String> fieldsMapping,
            SelectorsSupplier<K, V> selectorsSupplier) {

        class Support {

            private static Pattern FIELD_MAPPING = Pattern.compile(SelectorExpressionParser.SELECTION_REGEX);

            static String parseEntryValue(Entry<String, String> configEntry) {
                Matcher matcher = FIELD_MAPPING.matcher(configEntry.getValue());
                if (!matcher.matches()) {
                    ExpressionException.throwInvalidExpression(configEntry.getKey(), configEntry.getValue());
                }

                return matcher.group(1);
            }

            static String removePrefixFromEntryKey(Entry<String, String> configEntry) {
                String prefix = ConnectorConfig.FIELD + ".";
                String fieldConfigKey = configEntry.getKey();
                if (!fieldConfigKey.startsWith(prefix)) {
                    throw new RuntimeException(
                            "Unexpected format for field mapping key [%s]".formatted(fieldConfigKey));
                }
                return fieldConfigKey.substring(prefix.length());
            }

            static <K, V> void fill(Builder<K, V> builder, Entry<String, String> configEntry) {
                builder.withEntry(configEntry.getKey(), removePrefixFromEntryKey(configEntry), configEntry.getValue(),
                        parseEntryValue(configEntry));
            }
        }

        Builder<K, V> builder = Selectors.builder(selectorsSupplier).withSchemaName("fields");
        fieldsMapping.entrySet()
                .stream()
                .forEach(e -> Support.fill(builder, e));
        return new DefaultFieldMappings<>(builder.build());

    }

    static class DefaultFieldMappings<K, V> implements FieldMappings<K, V> {
        private Selectors<K, V> selectors;

        DefaultFieldMappings(Selectors<K, V> selectors) {
            this.selectors = selectors;
        }

        @Override
        public Selectors<K, V> selectors() {
            return selectors;
        }
    }
}