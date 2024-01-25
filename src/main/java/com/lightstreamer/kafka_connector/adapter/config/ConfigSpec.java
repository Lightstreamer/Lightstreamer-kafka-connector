
package com.lightstreamer.kafka_connector.adapter.config;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.lightstreamer.kafka_connector.adapter.config.ConfigSpec.DefaultHolder;

class ConfigSpec {

    interface Type {

        boolean isValid(String param);

        default String getValue(String param) {
            return param;
        }

        default String formatErrorMessage(String param, String paramValue) {
            return String.format("Value [%s] is not valid for parameter [%s]", paramValue, paramValue);
        }
    }

    static class Choices implements Type {

        private Set<String> choiches;

        Choices(String... choiches) {
            this.choiches = Set.copyOf(List.of(choiches));
        }

        @Override
        public boolean isValid(String param) {
            return choiches.contains(param);
        }

        static Choices booleans() {
            return new Choices("true", "false");
        }

    }

    enum ConfType implements Type {

        TEXT {
            @Override
            public boolean isValid(String paramValue) {
                return true;
            }
        },

        Int {
            @Override
            public boolean isValid(String param) {
                try {
                    Integer.valueOf(param);
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
        },

        Host {
            private static Pattern HOST = Pattern.compile("^([0-9a-zA-Z-.%_]+):([1-9]\\d*)$");

            @Override
            public boolean isValid(String paramValue) {
                return HOST.matcher(paramValue).matches();
            }
        },

        BOOL(Choices.booleans()) {

            @Override
            public boolean isValid(String param) {
                return embeddedType.isValid(param);
            }

        },

        URL {
            @Override
            public boolean isValid(String paramValue) {
                try {
                    URI uri = new URI(paramValue);
                    return uri.getHost() != null;
                } catch (Exception e) {
                    return false;
                }
            }
        },

        HostsList(Host) {
            @Override
            public boolean isValid(String param) {
                String[] params = param.split(",");
                return Arrays.stream(params).allMatch(embeddedType::isValid);
            }
        },

        ItemSpec {
            private static Pattern ITEM_SEPC = Pattern.compile("([a-zA-Z0-9_-]+)(-\\$\\{(.*)\\})?");

            @Override
            public boolean isValid(String param) {
                return ITEM_SEPC.matcher(param).matches();
            }

        },

        Directory {
            @Override
            public boolean isValid(String param) {
                return Files.isDirectory(Paths.get(param));
            };

            @Override
            public String getValue(String param) {
                return new File(param).getAbsolutePath();
            }

            @Override
            public String formatErrorMessage(String param, String paramValue) {
                return String.format("Directory [%s] not found", paramValue);
            }
        };

        Type embeddedType;

        ConfType() {
        }

        ConfType(Type t) {
            this.embeddedType = t;
        }

    }

    static class Either<Left, Right> {

        private final Left left;

        private final Right right;

        private Either(Left left, Right right) {
            if (left != null & right == null) {
                this.left = left;
                this.right = null;
                return;
            }
            if (left == null && right != null) {
                this.left = null;
                this.right = right;
                return;
            }
            throw new IllegalArgumentException("Only one parameter can be specified");
        }

        public Left getLeft() {
            return left;
        }

        public Right getRight() {
            return right;
        }

        public boolean isLeft() {
            return left != null;
        }

        public boolean isRight() {
            return right != null;
        }

        static <Left, Right> Either<Left, Right> left(Left left) {
            return new Either<>(left, null);
        }

        static <Left, Right> Either<Left, Right> right(Right right) {
            return new Either<>(null, right);
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

    private final Map<String, ConfParameter> paramSpec = new HashMap<>();

    ConfigSpec add(String name, boolean required, boolean multiple, Type type, boolean mutable,
            DefaultHolder<String> defaultValue) {
        paramSpec.put(name, new ConfParameter(name, required, multiple, null, type, mutable, defaultValue));
        return this;
    }

    ConfigSpec add(String name, boolean required, boolean multiple, Type type,
            DefaultHolder<String> defaultValue) {
        paramSpec.put(name, new ConfParameter(name, required, multiple, null, type, true, defaultValue));
        return this;
    }

    ConfigSpec add(String name, boolean required, boolean multiple, Type type) {
        paramSpec.put(name, new ConfParameter(name, required, multiple, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfigSpec add(String name, boolean required, boolean multiple, String suffix, Type type) {
        paramSpec.put(name,
                new ConfParameter(name, required, multiple, suffix, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfigSpec add(String name, boolean required, Type type) {
        paramSpec.put(name, new ConfParameter(name, required, false, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfigSpec add(String name, Type type) {
        paramSpec.put(name, new ConfParameter(name, true, false, null, type, true, DefaultHolder.defaultNull()));
        return this;
    }

    ConfParameter getParameter(String name) {
        return paramSpec.get(name);
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

        paramSpec.values()
                .stream()
                .filter(c -> c.mutable())
                .forEach(c -> c.populate(originals, parsedValues));

        return parsedValues;
    }
}

record ConfParameter(String name, boolean required, boolean multiple, String suffix, ConfigSpec.Type type,
        boolean mutable,
        DefaultHolder<String> defaultHolder) {

    String defaultValue() {
        return defaultHolder().value();
    }

    void populate(Map<String, String> source, Map<String, String> destination) throws ConfigException {
        // if (!mutable()) {
        // throw new ConfigException("Cannot modify parameter [%s]".formatted(name()));
        // }
        List<String> keys = Collections.singletonList(name());
        if (multiple()) {
            keys = source.keySet()
                    .stream()
                    .filter(key -> {
                        String[] components = key.split("\\.");
                        return components[0].equals(name())
                                && (suffix != null ? components[components.length - 1].equals(suffix) : true);
                    })
                    .toList();
            if (keys.isEmpty() && required()) {
                String templateReplacement = Optional.ofNullable(suffix()).map(s -> "." + s).orElse("");
                throw new ConfigException(
                        String.format("Specify at least one parameter [%s.<...>%s]", name, templateReplacement));
            }
        }

        for (String key : keys) {
            if (required()) {
                if (!source.containsKey(key)) {
                    throw new ConfigException(String.format("Missing required parameter [%s]", key));
                }
            }

            if (source.containsKey(key)) {
                String paramValue = source.get(key);
                if ((paramValue != null && !type.isValid(paramValue)) || paramValue == null || paramValue.isBlank()) {
                    throw new ConfigException(String.format("Specify a valid value for parameter [%s]", key));
                }
                destination.put(key, type.getValue(paramValue));
            }
        }
    }

}
