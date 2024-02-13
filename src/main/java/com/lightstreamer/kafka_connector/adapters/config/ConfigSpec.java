
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

package com.lightstreamer.kafka_connector.adapters.config;

import static com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.ConfType.BOOL;

import com.lightstreamer.kafka_connector.adapters.commons.Either;
import com.lightstreamer.kafka_connector.adapters.config.ConfigSpec.DefaultHolder;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.EvaluatorType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.KeystoreType;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.RecordErrorHandlingStrategy;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SaslMechanism;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SecurityProtocol;
import com.lightstreamer.kafka_connector.adapters.config.ConfigTypes.SslProtocol;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

class ConfigSpec {

    interface Type {

        boolean isValid(String param);

        default String getValue(String param) {
            return param;
        }

        default String formatErrorMessage(String param, String paramValue) {
            return String.format("Specify a valid value for parameter [%s]", param);
        }
    }

    static class Options implements Type {

        private Set<String> choiches;

        Options(String... options) {
            this.choiches = Set.of(options);
        }

        Options(Set<String> choiches) {
            this.choiches = Set.copyOf(choiches);
        }

        public String toString() {
            return choiches.toString();
        }

        @Override
        public boolean isValid(String param) {
            return choiches.contains(param);
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
    }

    static class ListType implements Type {

        private Type type;

        ListType(Type type) {
            this.type = type;
        }

        @Override
        public boolean isValid(String param) {
            String[] params = param.split(",");
            return Arrays.stream(params).allMatch(type::isValid);
        }
    }

    enum ConfType implements Type {
        TEXT,

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

        HOST {
            private static Pattern HOST = Pattern.compile("^([0-9a-zA-Z-.%_]+):([1-9]\\d*)$");

            @Override
            public boolean checkValidity(String param) {
                return HOST.matcher(param).matches();
            }
        },

        BOOL(Options.booleans()) {},

        EVALUATOR(Options.evaluatorTypes()),

        ERROR_STRATEGY(Options.errorStrategies()),

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

        ITEM_SPEC {
            private static Pattern ITEM_SEPC = Pattern.compile("([a-zA-Z0-9_-]+)(-\\$\\{(.*)\\})?");

            @Override
            public boolean checkValidity(String param) {
                return ITEM_SEPC.matcher(param).matches();
            }
        },

        FILE {
            @Override
            public boolean checkValidity(String param) {
                return Files.isRegularFile(Paths.get(param));
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
                return Files.isDirectory(Paths.get(param));
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

        SASL_MECHANISM(Options.saslMechanisms());

        Type embeddedType;

        ConfType() {}

        ConfType(Type t) {
            this.embeddedType = t;
        }

        @Override
        public final boolean isValid(String param) {
            if (embeddedType != null) {
                boolean valid = embeddedType.isValid(param);
                return valid;
            }
            return checkValidity(param);
        }

        public boolean checkValidity(String param) {
            return true;
        }
    }

    static class DefaultHolder<T> {

        Either<Supplier<T>, Function<Map<String, String>, T>> either;

        private DefaultHolder(Supplier<T> supplier) {
            this.either = Either.left(supplier);
        }

        private DefaultHolder(T value) {
            this(() -> value);
        }

        private DefaultHolder(Function<Map<String, String>, T> function) {
            this.either = Either.right(function);
        }

        public T value(Map<String, String> config) {
            if (either.isLeft()) {
                return either.getLeft().get();
            }
            return either.getRight().apply(config);
        }

        public T value() {
            return value(Collections.emptyMap());
        }

        static <T> DefaultHolder<T> defaultValue(T value) {
            return new DefaultHolder<>(value);
        }

        static <T> DefaultHolder<T> defaultValue(Supplier<T> supplier) {
            return new DefaultHolder<>(supplier);
        }

        static <T> DefaultHolder<T> defaultValue(Function<Map<String, String>, T> function) {
            return new DefaultHolder<>(function);
        }

        static <T> DefaultHolder<T> defaultNull() {
            return new DefaultHolder<>(() -> null);
        }
    }

    private final Map<String, ConfParameter> paramSpec = new LinkedHashMap<>();

    private Map<ConfParameter, ConfigSpec> subsections = new HashMap<>();

    ConfigSpec add(
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

    ConfigSpec add(
            String name,
            boolean required,
            boolean multiple,
            Type type,
            DefaultHolder<String> defaultValue) {
        paramSpec.put(
                name, new ConfParameter(name, required, multiple, null, type, true, defaultValue));
        return this;
    }

    ConfigSpec add(String name, boolean required, boolean multiple, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, required, multiple, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfigSpec add(String name, boolean required, boolean multiple, String suffix, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, required, multiple, suffix, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfigSpec add(String name, boolean required, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, required, false, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfigSpec add(String name, Type type) {
        paramSpec.put(
                name,
                new ConfParameter(
                        name, true, false, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfigSpec addConfigSpec(ConfigSpec spec, String enablingKey) {
        ConfParameter enablingPar = getParameter(enablingKey);
        if (enablingPar == null || !enablingPar.type().equals(BOOL)) {
            throw new ConfigException(
                    "Since no paramerter [%s] of type BOOL has been found, can't add parameters subsection"
                            .formatted(enablingKey));
        }
        subsections.put(enablingPar, spec);
        return this;
    }

    ConfParameter getParameter(String name) {
        ConfParameter confParameter = paramSpec.get(name);
        if (confParameter != null) {
            return confParameter;
        }
        Optional<ConfParameter> op =
                subsections.values().stream().map(c -> c.getParameter(name)).findFirst();
        if (op.isPresent()) {
            return op.get();
        }

        return null;
    }

    List<ConfParameter> getByType(Type type) {
        return paramSpec.values().stream().filter(p -> p.type().equals(type)).toList();
    }

    public static Optional<String> extractInfix(ConfParameter param, String value) {
        if (!param.multiple()) {
            return Optional.empty();
        }

        String infix = "";
        String prefix = param.name() + ".";
        boolean startsWith = value.startsWith(prefix);
        if (!startsWith) {
            return Optional.empty();
        }

        infix = value.substring(prefix.length());
        if (param.suffix() != null) {
            String suffix = "." + param.suffix();
            if (!infix.endsWith(suffix)) {
                return Optional.empty();
            }
            infix = infix.substring(0, infix.lastIndexOf(suffix));
        }
        return Optional.of(infix);
    }

    Map<String, String> parse(Map<String, String> originals) throws ConfigException {
        Map<String, String> parsedValues = new HashMap<>();

        paramSpec.values().stream()
                .filter(c -> c.mutable())
                .forEach(c -> c.populate(originals, parsedValues));

        subsections.forEach(
                (confParamater, confSpec) -> {
                    String enabled = parsedValues.getOrDefault(confParamater.name(), "false");
                    if (enabled.equals("true")) {
                        parsedValues.putAll(confSpec.parse(originals));
                    }
                });
        return parsedValues;
    }

    public ConfigSpec withEncryptionConfigs(String enableKey) {
        EncryptionConfigs.withEncryptionConfigs(this, enableKey);
        return this;
    }

    public ConfigSpec withKeystoreConfigs(String enableKey) {
        KeystoreConfigs.withKeystoreConfigs(this, enableKey);
        return this;
    }

    public ConfigSpec withAuthenticationConfigs(String enableKey) {
        AuthenticationConfigs.withAuthenticationConfigs(this, enableKey);
        return this;
    }
}

record ConfParameter(
        String name,
        boolean required,
        boolean multiple,
        String suffix,
        ConfigSpec.Type type,
        boolean mutable,
        DefaultHolder<String> defaultHolder) {

    String defaultValue() {
        return defaultHolder().value();
    }

    void populate(Map<String, String> source, Map<String, String> destination)
            throws ConfigException {
        List<String> keys = Collections.singletonList(name());
        if (multiple()) {
            keys =
                    source.keySet().stream()
                            .filter(
                                    key -> {
                                        String[] components = key.split("\\.");
                                        return components[0].equals(name())
                                                && (suffix != null
                                                        ? components[components.length - 1].equals(
                                                                suffix)
                                                        : true);
                                    })
                            .toList();
            if (keys.isEmpty() && required()) {
                String templateReplacement =
                        Optional.ofNullable(suffix()).map(s -> "." + s).orElse("");
                throw new ConfigException(
                        String.format(
                                "Specify at least one parameter [%s.<...>%s]",
                                name, templateReplacement));
            }
        }

        for (String key : keys) {
            if (required()) {
                if (!source.containsKey(key)) {
                    throw new ConfigException(
                            String.format("Missing required parameter [%s]", key));
                }
            }

            if (source.containsKey(key)) {
                String paramValue = source.get(key);
                if ((paramValue != null && !type.isValid(paramValue))
                        || paramValue == null
                        || paramValue.isBlank()) {
                    throw new ConfigException(type().formatErrorMessage(key, paramValue));
                }
                destination.put(key, type.getValue(paramValue));
            }
        }
    }
}
