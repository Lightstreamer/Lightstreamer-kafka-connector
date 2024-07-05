
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

    public static ExpressionException unexpectedTrailingDots(String name, String expression)
            throws ExpressionException {
        return new ExpressionException(
                "Found unexpected trailing dot(s) in the expression [%s] while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static ExpressionException invalidExpression(String name, String expression)
            throws ExpressionException {
        return new ExpressionException(
                "Found the invalid expression [%s] while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static void invalidFieldExpression(String fieldName, String expression)
            throws ExpressionException {
        throw new ExpressionException(
                "Found the invalid expression [%s] while evaluating [%s]: a valid expression must be enclosed within #{...}"
                        .formatted(expression, fieldName));
    }

    public static void wrapInvalidExpression(
            ExpressionException cause, String name, String expression) throws ExpressionException {
        throw new ExpressionException(
                "Found the invalid expression [%s] while evaluating [%s]: <%s>"
                        .formatted(expression, name, cause.getMessage()));
    }

    public static ExpressionException missingToken(String name, String expression)
            throws ExpressionException {
        return new ExpressionException(
                "Found the invalid expression [%s] with missing tokens while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static ExpressionException expectedRootToken(String name, String token)
            throws ExpressionException {
        return new ExpressionException(
                "Expected the root token [%s] while evaluating [%s]".formatted(token, name));
    }

    public static ExpressionException invalidIndexedExpression(String name, String expression)
            throws ExpressionException {
        return new ExpressionException(
                "Found the invalid indexed expression [%s] while evaluating [%s]"
                        .formatted(expression, name));
    }
}
