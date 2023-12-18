
package com.lightstreamer.kafka_connector.adapter.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

class ConfigSpec {

  interface Type {

    boolean isValid(String param);

    default String getValue(String param) {
      return param;
    }
  }

  static class ListType<T extends Type> implements Type {

    private final T type;

    public ListType(T t) {
      this.type = t;
    }

    @Override
    public boolean isValid(String param) {
      String[] params = param.split(",");
      return Arrays.stream(params).allMatch(type::isValid);
    }

  }

  enum ConfType implements Type {

    Text {
      public boolean isValid(String param) {
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
      public boolean isValid(String param) {
        Pattern p = Pattern.compile("^[a-zA-Z-_]+:[1-9]\\d*$");
        return p.matcher(param).matches();
      }
    },

    ItemSpec {

      private Pattern p = Pattern.compile("([a-zA-Z0-9_-]+)(-\\$\\{(.*)\\})?");

      public boolean isValid(String param) {
        System.out.println("ItemSpec validating " + param);
        return p.matcher(param).matches();
      }

    };

  }

  private Set<ConfParameter> paramSpec = new HashSet<>();

  ConfigSpec add(String name, boolean required, boolean multiple, Type type) {
    paramSpec.add(new ConfParameter(name, required, multiple, type));
    return this;
  }

  Map<String, String> parse(Map<String, String> params) throws ValidateException {
    Map<String, String> parsedValues = new HashMap<>();

    for (ConfParameter parameter : paramSpec) {
      parameter.populate(params, parsedValues);
    }
    return parsedValues;
  }
}

class ConfParameter {

  private String name;

  private ConfigSpec.Type type;

  private boolean required;

  private boolean multiple;

  ConfParameter(String name, boolean required, boolean multiple, ConfigSpec.Type type) {
    this.name = name;
    this.required = required;
    this.type = type;
    this.multiple = multiple;
  }

  void validate(String paramName, String paramValue) throws ValidateException {
    System.out.printf("Validating param <%s> = <%s>%n", paramName, paramValue);
    if (required) {
      if (paramValue == null || paramValue.isBlank()) {
        throw new ValidateException(String.format("Param [%s] is required", paramName));
      }
    }

    if (!type.isValid(paramValue)) {
      throw new ValidateException(String.format("Param [%s] is not valid", paramValue));
    }
  }

  boolean isMultiple() {
    return multiple;
  }

  void populate(Map<String, String> from, Map<String, String> to) throws ValidateException {
    List<String> keys = Collections.singletonList(name);
    if (isMultiple()) {
      keys = from.keySet()
          .stream()
          .filter(key -> key.startsWith(name)).toList();
    }

    for (String key : keys) {
      String paramValue = from.get(key);
      validate(key, paramValue);
      to.put(key, type.getValue(paramValue));
    }
  }

}
