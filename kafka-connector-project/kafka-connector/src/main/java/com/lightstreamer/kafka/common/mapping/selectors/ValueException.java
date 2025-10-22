
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

public class ValueException extends RuntimeException {

    private ValueException(String message) {
        super(message);
    }

    public static ValueException nullObject(String field) throws ValueException {
        return mkException("Cannot retrieve field [%s] from a null object", field);
    }

    public static ValueException scalarObject(String field) throws ValueException {
        return mkException("Cannot retrieve field [%s] from a scalar object", field);
    }

    public static ValueException nullObject(String nullField, int index) throws ValueException {
        return mkException("Cannot retrieve index [%d] from null object [%s]", index, nullField);
    }

    public static ValueException fieldNotFound(String field) throws ValueException {
        return mkException("Field [%s] not found", field);
    }

    public static ValueException indexOfOutBounds(int index) throws ValueException {
        return mkException("Field not found at index [%d]", index);
    }

    public static ValueException noIndexedField(String field) throws ValueException {
        return mkException("Field [%s] is not indexed", field);
    }

    public static ValueException noKeyFound(String key) {
        return mkException("Field not found at key ['%s']", key);
    }

    public static ValueException nonComplexObjectRequired(String expression) {
        return mkException("The expression [%s] must evaluate to a non-complex object", expression);
    }

    public static ValueException nonSchemaAssociated() throws ValueException {
        return mkException("A Schema is required");
    }

    private static ValueException mkException(String message, Object... args) {
        return new ValueException(message.formatted(args));
    }
}
