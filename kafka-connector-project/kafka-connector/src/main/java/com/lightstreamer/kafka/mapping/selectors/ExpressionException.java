
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

public class ExpressionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ExpressionException(String message) {
        super(message);
    }

    public ExpressionException(String message, Throwable cause) {
        super(message, cause);
    }

    public static void throwBlankToken(String name, String expression) throws ExpressionException {
        throw new ExpressionException(
                "Found the invalid expression [%s] with missing tokens while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static void throwExpectedRootToken(String name, String token)
            throws ExpressionException {
        throw new ExpressionException(
                "Expected the root token [%s] while evaluating [%s]".formatted(token, name));
    }

    public static void throwInvalidExpression(String name, String expression)
            throws ExpressionException {
        throw new ExpressionException(
                "Found the invalid expression [%s] while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static void throwInvalidIndexedExpression(String name, String expression)
            throws ExpressionException {
        throw new ExpressionException(
                "Found the invalid indexed expression [%s] while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static void reThrowInvalidExpression(
            ExpressionException cause, String name, String expression) throws ExpressionException {
        throw new ExpressionException(
                "Found the invalid expression [%s] while evaluating [%s]: <%s>"
                        .formatted(expression, name, cause.getMessage()));
    }
}
