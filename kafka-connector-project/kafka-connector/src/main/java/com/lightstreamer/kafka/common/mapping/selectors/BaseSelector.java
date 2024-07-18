
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

package com.lightstreamer.kafka.common.mapping.selectors;

import java.util.Objects;

public abstract class BaseSelector implements Selector {

    private final String name;

    private final String expression;

    protected BaseSelector(String name, String expression) {
        this.name = checkName(name);
        this.expression = checkExpression(expression);
    }

    protected String checkName(String name) {
        return Objects.requireNonNull(name);
    }

    protected String checkExpression(String expression) {
        return Objects.requireNonNull(expression);
    }

    @Override
    public String expression() {
        return expression;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        return obj instanceof BaseSelector other
                && Objects.equals(name, other.name)
                && Objects.equals(expression, other.expression);
    }
}
