
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

package com.lightstreamer.kafka.adapters.config.specs;

import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeFrom;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordConsumeWithOrderStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SecurityProtocol;
import com.lightstreamer.kafka.adapters.config.specs.ConfigTypes.SslProtocol;
import com.lightstreamer.kafka.common.config.ConfigException;
import com.lightstreamer.kafka.common.utils.Split;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ConfigsSpec {

    public interface Type {

        default boolean validate(String param) {
            if (param != null) {
                return isValid(param);
            }
            return false;
        }

        boolean isValid(String param);

        default String getValue(String param) {
            return param;
        }

        default String formatErrorMessage(String param, String paramValue) {
            return String.format(
                    "Specify a valid value for parameter [%s]", Objects.toString(param, "null"));
        }
    }

    public enum ConfType implements Type {
        CHAR {
            @Override
            public boolean checkValidity(String param) {
                return param != null && param.length() == 1;
            }
        },

        TEXT {
            @Override
            public boolean checkValidity(String param) {
                return param != null && !param.isBlank();
            }
        },

        BLANKABLE_TEXT {
            @Override
            public boolean checkValidity(String param) {
                return param != null;
            }
        },

        TEXT_LIST(new ListType(TEXT)),

        INT {
            @Override
            public boolean checkValidity(String param) {
                try {
                    Integer.valueOf(param);
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        },

        POSITIVE_INT {
            @Override
            public boolean checkValidity(String param) {
                try {
                    Integer value = Integer.valueOf(param);
                    return value > 0;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        },

        NON_NEGATIVE_INT {
            @Override
            public boolean checkValidity(String param) {
                try {
                    Integer value = Integer.valueOf(param);
                    return value >= 0;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        },

        THREADS {
            @Override
            public boolean checkValidity(String param) {
                try {
                    Integer threads = Integer.valueOf(param);
                    return threads == -1 || threads > 0;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        },

        HOST {
            private static Pattern HOST = Pattern.compile("^([0-9a-zA-Z-.%_]+):([1-9]\\d*)$");

            @Override
            public boolean checkValidity(String param) {
                return param != null && HOST.matcher(param).matches();
            }
        },

        BOOL(Options.booleans()) {},

        EVALUATOR(Options.evaluatorTypes()),

        CONSUME_FROM(Options.consumeEventsFrom()),

        ERROR_STRATEGY(Options.errorStrategies()),

        ORDER_STRATEGY(Options.orderStrategies()),

        SCHEMA_REGISTRY_PROVIDER(Options.schemaRegistryProviders()),

        URL {
            @Override
            public boolean checkValidity(String param) {
                try {
                    URI uri = new URI(param);
                    return uri.getHost() != null;
                } catch (Exception e) {
                    return false;
                }
            }
        },

        HOST_LIST(new ListType(HOST)),

        FILE {
            @Override
            public boolean checkValidity(String param) {
                return param != null && Files.isRegularFile(Paths.get(param));
            }

            @Override
            public String getValue(String param) {
                return new File(param).getAbsolutePath();
            }

            @Override
            public String formatErrorMessage(String param, String paramValue) {
                if (paramValue.isBlank()) {
                    return super.formatErrorMessage(param, paramValue);
                }
                return String.format("Not found file [%s] specified in [%s]", paramValue, param);
            }
        },

        DIRECTORY {
            @Override
            public boolean checkValidity(String param) {
                return param != null && Files.isDirectory(Paths.get(param));
            }

            @Override
            public String getValue(String param) {
                return new File(param).getAbsolutePath();
            }

            @Override
            public String formatErrorMessage(String param, String paramValue) {
                if (paramValue.isBlank()) {
                    return super.formatErrorMessage(param, paramValue);
                }
                return String.format(
                        "Not found directory [%s] specified in [%s]", paramValue, param);
            }
        },

        SECURITY_PROTOCOL(Options.securityProtocols()),

        KEYSTORE_TYPE(Options.keystoreTypes()),

        SSL_PROTOCOL(Options.sslProtocols()),

        SSL_ENABLED_PROTOCOLS(new ListType(new Options(ConfigTypes.SslProtocol.names()))),

        GROUP_MODE(Options.consumerGroupModes()),

        SASL_MECHANISM(Options.saslMechanisms());

        Type embeddedType;

        ConfType() {}

        ConfType(Type t) {
            this.embeddedType = t;
        }

        @Override
        public final boolean isValid(String param) {
            if (embeddedType != null) {
                return embeddedType.isValid(param);
            }
            return checkValidity(param);
        }

        public boolean checkValidity(String param) {
            return true;
        }
    }

    public static class DefaultHolder<T> {

        public static <T> DefaultHolder<T> defaultValue(T value) {
            return new DefaultHolder<>(value);
        }

        public static <T> DefaultHolder<T> defaultValue(Function<Map<String, String>, T> function) {
            return new DefaultHolder<>(function);
        }

        @SuppressWarnings("unchecked")
        public static <T> DefaultHolder<T> defaultNull() {
            return (DefaultHolder<T>) DefaultHolder.NULL;
        }

        @SuppressWarnings({"rawtypes"})
        private static final DefaultHolder NULL = new DefaultHolder<>(config -> null);

        Function<Map<String, String>, T> factory;

        private DefaultHolder(T value) {
            this(config -> value);
        }

        private DefaultHolder(Function<Map<String, String>, T> function) {
            this.factory = function;
        }

        @Override
        public int hashCode() {
            return Objects.hash(factory);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof DefaultHolder other && Objects.equals(factory, other.factory);
        }

        public T value(Map<String, String> config) {
            return factory.apply(config);
        }

        public T value() {
            return value(Collections.emptyMap());
        }
    }

    public static class EnablingKey {

        public static EnablingKey of(String... key) {
            return new EnablingKey(key);
        }

        private final List<String> keys;

        EnablingKey(String... key) {
            this.keys = List.of(key);
        }

        EnablingKey(List<String> keys) {
            this.keys = List.copyOf(keys);
        }

        public EnablingKey newNameSpaced(String ns) {
            return new EnablingKey(key().map(k -> ns + "." + k).toList());
        }

        public String toString() {
            return keys.toString();
        }

        Stream<String> key() {
            return keys.stream();
        }
    }

    public static record ConfParameter(
            String name,
            boolean required,
            boolean multiple,
            String suffix,
            Type type,
            boolean mutable,
            DefaultHolder<String> defaultHolder) {
        public ConfParameter {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Parameter name can't be null or blank");
            }

            if (type == null) {
                throw new IllegalArgumentException(
                        "Parameter type of [%s] can't be null".formatted(name));
            }

            if (multiple) {
                if (suffix != null && suffix.isBlank()) {
                    throw new IllegalArgumentException(
                            "Multiple parameter [%s] have a blank suffix".formatted(name));
                }

                if (defaultHolder != DefaultHolder.NULL) {
                    throw new IllegalArgumentException(
                            "Multiple parameters [%s] can't have a default value".formatted(name));
                }

                if (!mutable) {
                    throw new IllegalArgumentException(
                            "Multiple parameters [%s] must be mutable".formatted(name));
                }
            }

            if (!mutable) {
                if (!required) {
                    throw new IllegalArgumentException(
                            "Not mutable parameters [%s] must be required".formatted(name));
                }

                if (defaultHolder == DefaultHolder.NULL) {
                    throw new IllegalArgumentException(
                            "Not mutable parameter [%s] must have a non-null default value"
                                    .formatted(name));
                }
            }
        }

        public String defaultValue() {
            return defaultHolder().value();
        }

        private String fullName() {
            if (multiple()) {
                return "%s.<...>%s"
                        .formatted(
                                name(), Optional.ofNullable(suffix()).map(s -> "." + s).orElse(""));
            }
            return name();
        }

        void fill(Map<String, String> source, Map<String, String> destination)
                throws ConfigException {
            if (mutable()) {
                fillIfMutable(source, destination);
            } else {
                fillIfNotMutable(source, destination);
            }
        }

        private void fillIfNotMutable(Map<String, String> source, Map<String, String> destination) {
            String value = defaultHolder().value(source);
            if (value == null) {
                throw new ConfigException(
                        "Not mutable parameter [%s] must have a non-null default value"
                                .formatted(name()));
            }
            destination.put(name(), type.getValue(value));
        }

        private void fillIfMutable(Map<String, String> source, Map<String, String> destination)
                throws ConfigException {
            if (multiple()) {
                List<String> matchingKeys =
                        source.keySet().stream().filter(k -> isValidMultipleKey(k)).toList();
                if (matchingKeys.isEmpty() && required()) {
                    throw new ConfigException(
                            "Specify at least one parameter [%s]".formatted(fullName()));
                }

                for (String key : matchingKeys) {
                    Optional<String> infix = extractInfix(this, key);
                    if (infix.isPresent()) {
                        String paramValue = source.get(key);
                        validate(key, paramValue);
                        destination.put(key, type.getValue(paramValue));
                    } else {
                        throw new ConfigException(
                                "Specify a valid parameter [%s]".formatted(fullName()));
                    }
                }
                return;
            }

            final String paramValue;
            if (source.containsKey(name())) {
                paramValue = source.get(name());
            } else {
                // If the parameter is not present in the source, we try to get a default value,
                // possibly computed from the source itself.
                String defaultValue = defaultHolder().value(source);
                if (defaultValue == null) {
                    if (required()) {
                        throw new ConfigException(
                                "Missing required parameter [%s]".formatted(name()));
                    }
                    return;
                }
                paramValue = defaultValue;
            }
            validate(name(), paramValue);
            destination.put(name(), type.getValue(paramValue));
        }

        private boolean isValidMultipleKey(String key) {
            return (key.equals(name()) || key.startsWith(name() + "."))
                    && (suffix == null || key.endsWith("." + suffix));
        }

        private void validate(String key, String paramValue) {
            if (!type.validate(paramValue)) {
                throw new ConfigException(type().formatErrorMessage(key, paramValue));
            }
        }
    }

    static class Options implements Type {

        public static Options consumeEventsFrom() {
            return new Options(RecordConsumeFrom.names());
        }

        static Options booleans() {
            return new Options("true", "false");
        }

        static Options evaluatorTypes() {
            return new Options(EvaluatorType.names());
        }

        static Options errorStrategies() {
            return new Options(RecordErrorHandlingStrategy.names());
        }

        static Options orderStrategies() {
            return new Options(RecordConsumeWithOrderStrategy.names());
        }

        static Options securityProtocols() {
            return new Options(SecurityProtocol.names());
        }

        static Options keystoreTypes() {
            return new Options(KeystoreType.names());
        }

        static Options sslProtocols() {
            return new Options(SslProtocol.names());
        }

        static Options saslMechanisms() {
            return new Options(SaslMechanism.names());
        }

        static Options consumerGroupModes() {
            return new Options(ConfigTypes.ConsumerGroupMode.names());
        }

        static Options schemaRegistryProviders() {
            return new Options(ConfigTypes.SchemaRegistryProvider.names());
        }

        private Set<String> choices;

        Options(String... options) {
            this.choices = Set.of(options);
        }

        Options(Set<String> choices) {
            this.choices = Set.copyOf(choices);
        }

        public String toString() {
            return choices.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(choices);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof Options other && Objects.equals(choices, other.choices);
        }

        @Override
        public boolean isValid(String param) {
            return choices.contains(param);
        }
    }

    static class ListType implements Type {

        private Type type;

        ListType(Type type) {
            this.type = type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof ListType other && Objects.equals(type, other.type);
        }

        @Override
        public boolean isValid(String param) {
            return Split.byComma(param).stream().allMatch(type::isValid);
        }
    }

    private static record ChildSpec(
            EnablingKey enablingKey,
            ConfigsSpec spec,
            BiFunction<Map<String, String>, String, Boolean> evalStrategy) {}

    public static Optional<String> extractInfix(ConfParameter param, String entry) {
        final Optional<String> EMPTY = Optional.empty();
        if (!param.multiple()) {
            return EMPTY;
        }

        String prefix = param.name() + ".";
        if (!entry.startsWith(prefix)) {
            return EMPTY;
        }

        int endIndex =
                Optional.ofNullable(param.suffix())
                        .map(
                                suffix ->
                                        entry.endsWith(suffix)
                                                ? entry.lastIndexOf("." + suffix)
                                                : -1)
                        .orElse(entry.length());
        if (endIndex < prefix.length()) {
            return EMPTY;
        }
        String infix = entry.substring(prefix.length(), endIndex);
        if (infix.isBlank()) {
            return EMPTY;
        }
        return Optional.of(infix);
    }

    private final Map<String, ConfParameter> paramSpec = new LinkedHashMap<>();
    private final List<ChildSpec> specChildren = new ArrayList<>();
    private final String name;

    public ConfigsSpec(String name) {
        this.name = name;
    }

    public ConfigsSpec() {
        this.name = null;
    }

    public ConfigsSpec(ConfigsSpec from) {
        this.name = from.name;
        from.paramSpec.forEach(
                (name, confParameter) -> {
                    ConfParameter p =
                            new ConfParameter(
                                    confParameter.name(),
                                    confParameter.required(),
                                    confParameter.multiple(),
                                    confParameter.suffix(),
                                    confParameter.type(),
                                    confParameter.mutable(),
                                    confParameter.defaultHolder());
                    paramSpec.put(name, p);
                });
        for (ChildSpec childSpec : from.specChildren) {
            withChildConfigs(childSpec.spec(), childSpec.evalStrategy(), childSpec.enablingKey());
        }
    }

    public ConfigsSpec withChildConfigs(ConfigsSpec childConfigSpec) {
        for (ConfParameter cp : childConfigSpec.paramSpec.values()) {
            add(
                    new ConfParameter(
                            cp.name(),
                            cp.required(),
                            cp.multiple(),
                            cp.suffix(),
                            cp.type(),
                            cp.mutable(),
                            cp.defaultHolder()));
        }
        for (ChildSpec childSpec : childConfigSpec.specChildren) {
            withChildConfigs(childSpec.spec(), childSpec.evalStrategy(), childSpec.enablingKey());
        }

        return this;
    }

    public ConfigsSpec newSpecWithNameSpace(String nameSpace) {
        ConfigsSpec newSpec = new ConfigsSpec(this.name);
        for (ConfParameter cp : paramSpec.values()) {
            newSpec.add(
                    new ConfParameter(
                            nameSpace + "." + cp.name(),
                            cp.required(),
                            cp.multiple(),
                            cp.suffix(),
                            cp.type(),
                            cp.mutable(),
                            cp.defaultHolder()));
        }
        for (ChildSpec childSpec : specChildren) {
            ConfigsSpec spec = childSpec.spec();
            newSpec.withChildConfigs(
                    spec.newSpecWithNameSpace(nameSpace),
                    childSpec.evalStrategy(),
                    childSpec.enablingKey().newNameSpaced(nameSpace));
        }

        return newSpec;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, paramSpec, specChildren);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        return obj instanceof ConfigsSpec other
                && Objects.equals(name, other.name)
                && Objects.equals(paramSpec, other.paramSpec)
                && Objects.equals(specChildren, other.specChildren);
    }

    public ConfigsSpec add(
            String name,
            boolean required,
            boolean multiple,
            Type type,
            boolean mutable,
            DefaultHolder<String> defaultValue) {
        paramSpec.put(
                name,
                new ConfParameter(name, required, multiple, null, type, mutable, defaultValue));
        return this;
    }

    public ConfigsSpec add(
            String name,
            boolean required,
            boolean multiple,
            Type type,
            DefaultHolder<String> defaultValue) {
        paramSpec.put(
                name, new ConfParameter(name, required, multiple, null, type, true, defaultValue));
        return this;
    }

    public ConfigsSpec add(String name, boolean required, boolean multiple, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, required, multiple, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    public ConfigsSpec add(
            String name, boolean required, boolean multiple, String suffix, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, required, multiple, suffix, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    public ConfigsSpec add(String name, boolean required, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, required, false, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    public ConfigsSpec add(String name, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, true, false, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    public ConfigsSpec withEnabledChildConfigs(ConfigsSpec spec, String enablingKey) {
        return withEnabledChildConfigs(
                spec, (map, key) -> map.getOrDefault(key, "false").equals("true"), enablingKey);
    }

    public ConfigsSpec withEnabledChildConfigs(ConfigsSpec spec, EnablingKey enablingKey) {
        return withChildConfigs(
                spec, (map, key) -> map.getOrDefault(key, "false").equals("true"), enablingKey);
    }

    public ConfigsSpec withEnabledChildConfigs(
            ConfigsSpec spec,
            BiFunction<Map<String, String>, String, Boolean> evalStrategy,
            String enablingKey) {
        return withChildConfigs(spec, evalStrategy, new EnablingKey(enablingKey));
    }

    private ConfigsSpec withChildConfigs(
            ConfigsSpec spec,
            BiFunction<Map<String, String>, String, Boolean> evalStrategy,
            EnablingKey enablingKey) {
        enablingKey
                .key()
                .map(s -> lookUpParameter(s))
                .filter(Objects::nonNull)
                .findAny()
                .orElseThrow(
                        () ->
                                new ConfigException(
                                        "Can't add parameters subsection [%s] without the enabling parameter [%s]"
                                                .formatted(spec.name, enablingKey)));
        specChildren.add(new ChildSpec(enablingKey, spec, evalStrategy));
        return this;
    }

    public ConfParameter findParameter(String name) {
        ConfParameter parameter = lookUpParameter(name);
        if (parameter != null) {
            return parameter;
        }

        for (ChildSpec trigger : specChildren) {
            parameter = trigger.spec().findParameter(name);
            if (parameter != null) {
                return parameter;
            }
        }

        // No parameter found
        return null;
    }

    private ConfParameter lookUpParameter(String name) {
        return paramSpec.get(name);
    }

    public List<ConfParameter> getByType(Type type) {
        return Stream.concat(
                        paramSpec.values().stream().filter(p -> p.type().equals(type)),
                        specChildren.stream().flatMap(s -> s.spec().getByType(type).stream()))
                .toList();
    }

    public Map<String, String> parse(Map<String, String> originals) throws ConfigException {
        // Final map containing all parsed values.
        Map<String, String> configurations = new HashMap<>();

        for (ConfParameter param : paramSpec.values()) {
            param.fill(originals, configurations);
        }

        // Populate the map iterating over the children specs.
        for (ChildSpec trigger : specChildren) {
            Stream<String> key = trigger.enablingKey.key();
            boolean enabled = key.anyMatch(k -> trigger.evalStrategy().apply(configurations, k));
            if (enabled) {
                configurations.putAll(trigger.spec().parse(originals));
            }
        }
        return configurations;
    }

    ConfigsSpec add(ConfParameter confParameter) {
        paramSpec.put(confParameter.name(), confParameter);
        return this;
    }
}
