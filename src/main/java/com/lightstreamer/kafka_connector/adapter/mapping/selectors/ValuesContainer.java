/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

import java.util.Set;

public interface ValuesContainer {

  Selectors<?, ?> selectors();

  Set<Value> values();
}

record DefaultValuesContainer(Selectors<?, ?> selectors, Set<Value> values)
    implements ValuesContainer {}
