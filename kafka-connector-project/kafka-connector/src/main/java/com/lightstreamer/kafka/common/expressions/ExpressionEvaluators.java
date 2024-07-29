
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExpressionEvaluators {

    public interface Expression {}

    public interface ExpressionEvaluator {

        Expression eval(String expression);
    }

    public interface TemplateEvaluator extends ExpressionEvaluator {

        @Override
        TemplateExpression eval(String expression);
    }

    public interface ItemEvaluator extends ExpressionEvaluator {

        @Override
        SubscriptionExpression eval(String expression);
    }

    public interface FieldEvaluator extends ExpressionEvaluator {

        @Override
        ExtractionExpression eval(String expression);
    }

    public record TemplateExpression(String prefix, Map<String, ExtractionExpression> params)
            implements Expression {}

    public record SubscriptionExpression(String prefix, Map<String, String> params)
            implements Expression {}

    public record ExtractionExpression(String expression) implements Expression {

        @Override
        public String toString() {
            return expression();
        }

        public static ExtractionExpression of(String expression) {
            return new ExtractionExpression(expression);
        }
    }

    enum Patterns {
        TEMPLATE(
                Pattern.compile("(^[a-zA-Z0-9_-]+)(-" + SELECTION_REGEX + ")$"),
                Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?")),

        SUBSCRIBED(
                Pattern.compile("([a-zA-Z0-9_-]+)(-\\[(.*)\\])?"),
                Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?"));

        private Pattern gobalPattern;
        private Pattern localPattern;

        Patterns(Pattern global, Pattern local) {
            this.gobalPattern = global;
            this.localPattern = local;
        }

        Pattern globalPattern() {
            return this.gobalPattern;
        }

        Pattern localPattern() {
            return this.localPattern;
        }

        String errorMessage() {
            return switch (this) {
                case TEMPLATE -> "Invalid template expression";
                case SUBSCRIBED -> "Invalid Item";
            };
        }
    }

    private static class FieldExpressionEvaluator implements FieldEvaluator {

        private static final Pattern PATTERN = Pattern.compile(SELECTION_REGEX);

        @Override
        public ExtractionExpression eval(String expression) {
            Matcher matcher = PATTERN.matcher(Objects.toString(expression, ""));
            if (!matcher.matches()) {
                throw new ExpressionException("Invalid field expression");
            }
            return new ExtractionExpression(matcher.group(1));
        }
    }

    private static class TemplateExpressionEvaluator implements TemplateEvaluator {

        private final Patterns patterns = Patterns.TEMPLATE;

        private TemplateExpressionEvaluator() {}

        @Override
        public TemplateExpression eval(String expression) throws ExpressionException {
            Matcher globalMatcher = patterns.globalPattern().matcher(expression);
            if (!globalMatcher.matches()) {
                throw new ExpressionException(patterns.errorMessage());
            }
            Map<String, ExtractionExpression> queryParams = new HashMap<>();
            String prefix = globalMatcher.group(1);
            String queryString = globalMatcher.group(3);
            if (queryString != null) {
                Matcher localMatcher = patterns.localPattern().matcher(queryString);
                int previousEnd = 0;
                while (localMatcher.find()) {
                    if (localMatcher.start() != previousEnd) {
                        break;
                    }
                    String key = localMatcher.group(2);
                    String value = localMatcher.group(3);
                    if (queryParams.put(key, new ExtractionExpression(value)) != null) {
                        throw new ExpressionException("No duplicated keys are allowed");
                    }
                    previousEnd = localMatcher.end();
                }
                if (previousEnd < queryString.length()) {
                    throw new ExpressionException(patterns.errorMessage());
                }
            }

            return new TemplateExpression(prefix, queryParams);
        }
    }

    private static class ItemExpressionEvaluator implements ItemEvaluator {

        private final Patterns patterns = Patterns.SUBSCRIBED;

        private ItemExpressionEvaluator() {}

        @Override
        public SubscriptionExpression eval(String expression) throws ExpressionException {
            Matcher globalMatcher = patterns.globalPattern().matcher(expression);
            if (!globalMatcher.matches()) {
                throw new ExpressionException(patterns.errorMessage());
            }
            Map<String, String> queryParams = new HashMap<>();
            String prefix = globalMatcher.group(1);
            String queryString = globalMatcher.group(3);
            if (queryString != null) {
                Matcher localMatcher = patterns.localPattern().matcher(queryString);
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
                    throw new ExpressionException(patterns.errorMessage());
                }
            }

            return new SubscriptionExpression(prefix, queryParams);
        }
    }

    private static final String SELECTION_REGEX = "\\#\\{(.+)\\}";

    static TemplateEvaluator TEMPLATE_EVALUATOR = new TemplateExpressionEvaluator();
    static ItemExpressionEvaluator SUBSCRIPTION_EVALUATOR = new ItemExpressionEvaluator();
    static FieldExpressionEvaluator FIELD_EVALUATOR = new FieldExpressionEvaluator();

    public static TemplateEvaluator template() {
        return TEMPLATE_EVALUATOR;
    }

    public static ItemEvaluator subscription() {
        return SUBSCRIPTION_EVALUATOR;
    }

    public static FieldEvaluator field() {
        return FIELD_EVALUATOR;
    }

    private ExpressionEvaluators() {}
}
