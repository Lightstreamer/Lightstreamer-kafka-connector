
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

package com.lightstreamer.kafka.common.expressions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Expressions {

    public interface Expression {}

    public interface ExtractionExpression extends Expression {

        Constant constant();

        String[] tokens();

        String expression();
    }

    public record TemplateExpression(String prefix, Map<String, ExtractionExpression> params)
            implements Expression {}

    public record SubscriptionExpression(String prefix, Map<String, String> params)
            implements Expression {}

    private static class ExtractionExpressionImpl implements ExtractionExpression {

        private final Constant root;
        private final String completeExpression;
        private final String[] tokens;

        private ExtractionExpressionImpl(Constant root, String[] tokens, String expression) {
            this.root = root;
            this.completeExpression = expression;
            this.tokens = tokens;
        }

        @Override
        public String toString() {
            return expression();
        }

        @Override
        public Constant constant() {
            return root;
        }

        @Override
        public String expression() {
            return completeExpression;
        }

        public String[] tokens() {
            return Arrays.copyOf(tokens, tokens.length);
        }

        @Override
        public int hashCode() {
            return Objects.hash(completeExpression);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof ExtractionExpressionImpl other
                    && Objects.equals(completeExpression, other.completeExpression);
        }
    }

    private static final String SELECTION_REGEX = "\\#\\{(.+)\\}";

    private static Pattern TEMPLATE_GLOBAL =
            Pattern.compile("(^[a-zA-Z0-9_-]+)(-" + SELECTION_REGEX + ")$");
    private static Pattern TEMPLATE_LOCAL =
            Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?");
    private static Pattern SUBSCRIBED_GLOBAL = Pattern.compile("([a-zA-Z0-9_-]+)(-\\[(.*)\\])?");
    private static Pattern SUBSCRIBED_LOCAL = Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?");
    private static Pattern FIELD = Pattern.compile(SELECTION_REGEX);

    private static final Expressions EXPRESSIONS = new Expressions();

    public static TemplateExpression template(String templateExpression) {
        return EXPRESSIONS.newTemplateExpression(templateExpression);
    }

    public static ExtractionExpression wrapped(String wrappedExpression) {
        return EXPRESSIONS.fromWrapped(wrappedExpression);
    }

    public static SubscriptionExpression subscription(String subscriptionExpression) {
        return EXPRESSIONS.newSubscriptionExpression(subscriptionExpression);
    }

    public static ExtractionExpression expression(String expression) {
        return EXPRESSIONS.newExpression(expression);
    }

    private static String[] getTokens(String expression) {
        // expression.splitWithDelimiters("\\.", 0); // Valid for Java 21
        StringTokenizer st = new StringTokenizer(expression, ".", true);
        String[] tokens = new String[st.countTokens()];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = st.nextToken();
        }
        return tokens;
    }

    private Expressions() {}

    TemplateExpression newTemplateExpression(String templateExpression) {
        Matcher globalMatcher = TEMPLATE_GLOBAL.matcher(nonNullString(templateExpression));
        if (!globalMatcher.matches()) {
            throw new ExpressionException("Invalid template expression");
        }
        Map<String, ExtractionExpression> queryParams = new HashMap<>();
        String prefix = globalMatcher.group(1);
        String queryString = globalMatcher.group(3);
        if (queryString != null) {
            Matcher localMatcher = TEMPLATE_LOCAL.matcher(queryString);
            int previousEnd = 0;
            while (localMatcher.find()) {
                if (localMatcher.start() != previousEnd) {
                    break;
                }
                String key = localMatcher.group(2);
                String value = localMatcher.group(3);
                if (queryParams.put(key, newExpression(value)) != null) {
                    throw new ExpressionException("No duplicated keys are allowed");
                }
                previousEnd = localMatcher.end();
            }
            if (previousEnd < queryString.length()) {
                throw new ExpressionException("Invalid template expression");
            }
        }

        return new TemplateExpression(prefix, queryParams);
    }

    SubscriptionExpression newSubscriptionExpression(String subscriptionExpression) {
        Matcher globalMatcher = SUBSCRIBED_GLOBAL.matcher(nonNullString(subscriptionExpression));
        if (!globalMatcher.matches()) {
            throw new ExpressionException("Invalid Item");
        }
        Map<String, String> queryParams = new HashMap<>();
        String prefix = globalMatcher.group(1);
        String queryString = globalMatcher.group(3);
        if (queryString != null) {
            Matcher localMatcher = SUBSCRIBED_LOCAL.matcher(queryString);
            int previousEnd = 0;
            while (localMatcher.find()) {
                if (localMatcher.start() != previousEnd) {
                    break;
                }
                String key = localMatcher.group(2);
                String value = localMatcher.group(3);
                if (queryParams.put(key, value) != null) {
                    throw new ExpressionException("No duplicated keys are allowed");
                }
                previousEnd = localMatcher.end();
            }
            if (previousEnd < queryString.length()) {
                throw new ExpressionException("Invalid Item");
            }
        }

        return new SubscriptionExpression(prefix, queryParams);
    }

    ExtractionExpressionImpl fromWrapped(String wrappedExpression) {
        Matcher matcher = FIELD.matcher(nonNullString(wrappedExpression));
        if (!matcher.matches()) {
            throw new ExpressionException("Invalid expression");
        }
        return newExpression(matcher.group(1));
    }

    ExtractionExpressionImpl newExpression(String expression) throws ExpressionException {
        String[] tokens = getTokens(nonNullString(expression));
        Constant root = tokens.length > 0 ? Constant.from(tokens[0]) : null;
        if (root == null) {
            throw ExpressionException.missingRootTokens(expression);
        }
        if (tokens.length > 0 && tokens[tokens.length - 1].equals(".")) {
            throw ExpressionException.unexpectedTrailingDots(expression);
        }
        return new ExtractionExpressionImpl(root, tokens, expression);
    }

    static String nonNullString(String str) {
        return Objects.toString(str, "");
    }
}
