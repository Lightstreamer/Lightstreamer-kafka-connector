
package com.lightstreamer.kafka_connector.adapter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

interface Type {
  boolean isValid(String param);
}

class ListType<T extends Type> implements Type {

  private T type;

  ListType(T t) {
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
      Pattern p = Pattern.compile("^[a-zA_Z-_]+:[1-9]\\d*$");
      return p.matcher(param).matches();
    }
  },
}

class ConfValue<T> {

  private T value;

  ConfValue(T value) {
    this.value = value;
  }

  T getValue() {
    return value;

  }
}

class ConfParameter {

  private String name;

  private Type type;

  private boolean required;

  ConfParameter(String name, boolean required, Type type) {
    this.name = name;
    this.required = required;
    this.type = type;
  }

  public void validate(String paramValue) throws ValidateException {
    System.out.println("Validating " + paramValue);
    if (required) {
      if (paramValue == null || paramValue.isBlank()) {
        throw new ValidateException(String.format("Param [%s] is required", name));
      }
    }

    if (!type.isValid(paramValue)) {
      throw new ValidateException(String.format("Param [%s] is not valid", paramValue));
    }
  }
}

public class ConfigSpec {

  private Map<String, ConfParameter> paramSpec = new HashMap<>();

  ConfigSpec add(String name, boolean required, Type type) {
    paramSpec.put(name, new ConfParameter(name, required, type));
    return this;
  }

  Map<String, String>  parse(Map<String, String> params) throws ValidateException {
    Map<String, String> parsedValues = new HashMap<>();
    Set<Entry<String, ConfParameter>> set = paramSpec.entrySet();
    for (Entry<String, ConfParameter> entry : set) {
      String paramValue = params.get(entry.getKey());
      entry.getValue().validate(paramValue);
      parsedValues.put(entry.getKey(), paramValue);
    }
    return parsedValues;
  }
}
