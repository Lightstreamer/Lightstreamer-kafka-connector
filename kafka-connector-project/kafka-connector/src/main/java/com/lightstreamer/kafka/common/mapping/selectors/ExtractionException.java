
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

public class ExtractionException extends Exception {

    private static final long serialVersionUID = 1L;

    private ExtractionException(String message) {
        super(message);
    }

    public static ExtractionException expectedRootToken(String name, String expectedRoot)
            throws ExtractionException {
        return new ExtractionException(
                "Expected the root token [%s] while evaluating [%s]".formatted(expectedRoot, name));
    }

    public static ExtractionException invalidExpression(String name, String expression)
            throws ExtractionException {
        return new ExtractionException(
                "Got the following error while evaluating [%s] while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static ExtractionException missingToken(String name, String expression)
            throws ExtractionException {
        return new ExtractionException(
                "Found the invalid expression [%s] with missing tokens while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static ExtractionException missingAttribute(String name, String expression)
            throws ExtractionException {
        return new ExtractionException(
                "Found the invalid expression [%s] with missing attribute while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static ExtractionException notAllowedAttributes(String name, String expression)
            throws ExtractionException {
        return new ExtractionException(
                "Found the invalid expression [%s] for scalar values while evaluating [%s]"
                        .formatted(expression, name));
    }

    public static ExtractionException invalidIndexedExpression(String name, String expression)
            throws ExtractionException {
        return new ExtractionException(
                "Found the invalid indexed expression [%s] while evaluating [%s]"
                        .formatted(expression, name));
    }
}
