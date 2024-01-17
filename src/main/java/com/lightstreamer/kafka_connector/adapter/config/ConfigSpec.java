
package com.lightstreamer.kafka_connector.adapter.config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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

    enum ConfType implements Type {

        Text {
            public boolean isValid(String paramValue) {
                return true;
            }
        },

        Int {
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
            private static Pattern HOST = Pattern.compile("^[a-zA-Z-_]+:[1-9]\\d*$");

            public boolean isValid(String paramValue) {
                return HOST.matcher(paramValue).matches();
            }
        },

        HostsList(Host) {

            @Override
            public boolean isValid(String param) {
                String[] params = param.split(",");
                return Arrays.stream(params).allMatch(embeddedTYpe::isValid);
            }
        },

        ItemSpec {

            private static Pattern ITEM_SEPC = Pattern.compile("([a-zA-Z0-9_-]+)(-\\$\\{(.*)\\})?");

            public boolean isValid(String param) {
                return ITEM_SEPC.matcher(param).matches();
            }

        },

        Directory {

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

        Type embeddedTYpe;

        ConfType() {

        }

        ConfType(Type t) {
            this.embeddedTYpe = t;
        }

    }

    private Map<String, ConfParameter> paramSpec = new HashMap<>();

    ConfigSpec add(String name, boolean required, boolean multiple, Type type, String defaultValue) {
        paramSpec.put(name, new ConfParameter(name, required, multiple, type, defaultValue));
        return this;
    }

    ConfigSpec add(String name, boolean required, boolean multiple, Type type) {
        paramSpec.put(name, new ConfParameter(name, required, multiple, type));
        return this;
    }

    ConfigSpec add(String name, boolean required, Type type) {
        paramSpec.put(name, new ConfParameter(name, required, false, type));
        return this;
    }

    ConfigSpec add(String name, Type type) {
        paramSpec.put(name, new ConfParameter(name, true, false, type));
        return this;
    }

    ConfParameter getParameter(String name) {
        return paramSpec.get(name);
    }

    Map<String, String> parse(Map<String, String> params) throws ConfigException {
        Map<String, String> parsedValues = new HashMap<>();

        for (ConfParameter parameter : paramSpec.values()) {
            parameter.populate(params, parsedValues);
        }
        return parsedValues;
    }
}

record ConfParameter(String name, boolean required, boolean multiple, ConfigSpec.Type type, String defaultValue) {

    ConfParameter(String name, boolean required, boolean multiple, ConfigSpec.Type type) {
        this(name, required, multiple, type, null);
    }

    void validate(String paramName, String paramValue) throws ConfigException {
        if (required()) {
            if (paramValue == null || paramValue.isBlank()) {
                throw new ConfigException(String.format("Param [%s] is required", paramName));
            }
        }

        if (!type.isValid(paramValue)) {
            throw new ConfigException(type.formatErrorMessage(paramName, paramValue));
        }
    }

    void populate(Map<String, String> source, Map<String, String> destination) throws ConfigException {
        List<String> keys = Collections.singletonList(name());
        if (multiple()) {
            keys = source.keySet()
                    .stream()
                    .filter(key -> key.startsWith(name()) && name().length() < key.length())
                    .toList();
            if (keys.isEmpty() && required()) {
                throw new ConfigException(
                        String.format("At least one param [%s<...>] is required", name));
            }
        }

        for (String key : keys) {
            if (required()) {
                if (!source.containsKey(key)) {
                    throw new ConfigException(String.format("Param [%s] is required", key));
                }
            }

            if (source.containsKey(key)) {
                String paramValue = source.get(key);
                if (paramValue == null || paramValue.isBlank()) {
                    throw new ConfigException(String.format("You must specify a value for Param [%s]", key));
                }
                destination.put(key, type.getValue(paramValue));
            }
        }
    }

}
