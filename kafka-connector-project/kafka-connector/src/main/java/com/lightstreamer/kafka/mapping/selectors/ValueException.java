
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

package com.lightstreamer.kafka.mapping.selectors;

public class ValueException extends RuntimeException {

    public ValueException(String message) {
        super(message);
    }

    public static void throwNullObject(String field) throws ValueException {
        throwException(mkException("Field [%s] is NULL", field));
    }

    public static void throwFieldNotFound(String field) throws ValueException {
        throwException(mkException("Field [%s] not found", field));
    }

    public static void throwIndexOfOutBoundex(int index) throws ValueException {
        throwException(mkException("Field not found at index [%d]", index));
    }

    public static void throwNoIndexedField() throws ValueException {
        throwException(mkException("Current field is not indexed"));
    }

    public static void throwNoKeyFound(String key) {
        throwException(mkException("Field not found at key ['%s']", key));
    }

    public static void throwConversionError(String type) throws ValueException {
        throwException(mkException("Current field [%s] is a terminal object", type));
    }

    public static void throwNonComplexObjectRequired(String expression) {
        throwException(
                mkException(
                        "The expression [%s] must evaluate to a non-complex object", expression));
    }

    private static void throwException(ValueException ve) throws ValueException {
        throw ve;
    }

    private static ValueException mkException(String message, Object... args) {
        return new ValueException(message.formatted(args));
    }
}
