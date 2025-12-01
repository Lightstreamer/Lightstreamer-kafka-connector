
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Expressions {

    // ================================
    // PUBLIC INTERFACES
    // ================================

    public interface Expression {}

    public interface ExtractionExpression extends Expression {

        Constant constant();

        String[] tokens();

        String expression();
    }

    public enum Constant implements ExtractionExpression {
        KEY(true),
        VALUE(true),
        TIMESTAMP,
        PARTITION,
        OFFSET,
        TOPIC,
        HEADERS(true);

        private static final Map<String, Constant> NAME_CACHE;

        static {
            NAME_CACHE = Stream.of(values()).collect(toMap(Constant::toString, identity()));
        }

        public static final String VALUES_STR =
                Arrays.stream(Constant.values())
                        .map(a -> a.toString())
                        .collect(Collectors.joining("|"));

        public static Constant from(String name) {
            boolean indexed = false;
            if (name.endsWith("]") && name.contains("[")) {
                indexed = true;
                // Handle indexed constants like "KEY[0]"
                name = name.substring(0, name.indexOf('['));
            }
            Constant constant = NAME_CACHE.get(name);
            if (constant != null && !constant.allowIndex() && indexed) {
                return null;
            }
            return constant;
        }

        static Set<Constant> all() {
            return Arrays.stream(values()).collect(Collectors.toCollection(LinkedHashSet::new));
        }

        private final String[] tokens = new String[1];
        private final boolean allowIndex;

        Constant(boolean allowIndex) {
            this.allowIndex = allowIndex;
            Arrays.fill(tokens, toString());
        }

        Constant() {
            this(false);
        }

        @Override
        public Constant constant() {
            return this;
        }

        @Override
        public String[] tokens() {
            return tokens;
        }

        @Override
        public String expression() {
            return toString();
        }

        public boolean allowIndex() {
            return allowIndex;
        }
    }

    public record TemplateExpression(String prefix, Map<String, ExtractionExpression> params) {}

    public record SubscriptionExpression(String prefix, SortedSet<Data> dataSet) {

        public Schema schema() {
            return Schema.from(
                    prefix, dataSet.stream().map(Data::name).collect(Collectors.toSet()));
        }

        public String asCanonicalItemName() {
            return Data.buildItemName(dataSet.toArray(new Data[0]), prefix());
        }
    }

    public static class ExpressionException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public ExpressionException(String message) {
            super(message);
        }

        public static ExpressionException missingRootTokens(String expression)
                throws ExpressionException {
            return new ExpressionException(
                    "Missing root tokens [%s]".formatted(Constant.VALUES_STR));
        }

        public static ExpressionException unexpectedTrailingDots(String expression)
                throws ExpressionException {
            return new ExpressionException(
                    "Found unexpected trailing dot(s) in the expression [%s]"
                            .formatted(expression));
        }
    }

    // ================================
    // PRIVATE NESTED CLASSES & RECORDS
    // ================================

    private static final class Parser<R> {

        interface ParseResultBuilder<R> {

            void notifyPrefix(String prefix);

            boolean notifyParam(String key, String value);

            R build();
        }

        private static final Pattern PARAM = Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?");

        private final Pattern globalPattern;
        private final Supplier<ParseResultBuilder<R>> builderSupplier;
        private final String errorMsg;

        private Parser(ParserBuilder<R> builder) {
            this.globalPattern = builder.globalPattern;
            this.builderSupplier = builder.resultBuilder;
            this.errorMsg = builder.errorMsg;
        }

        R parse(String expression) {
            Matcher globalMatcher = globalPattern.matcher(nonNullString(expression));
            if (!globalMatcher.matches()) {
                throw new ExpressionException(errorMsg);
            }

            ParseResultBuilder<R> resultBuilder = this.builderSupplier.get();
            resultBuilder.notifyPrefix(globalMatcher.group(1));
            String queryString = globalMatcher.group(3);
            if (queryString != null) {
                Matcher localMatcher = PARAM.matcher(queryString);
                int previousEnd = 0;
                while (localMatcher.find()) {
                    if (localMatcher.start() != previousEnd) {
                        break;
                    }
                    String key = localMatcher.group(2);
                    String value = localMatcher.group(3);
                    if (!resultBuilder.notifyParam(key, value)) {
                        throw new ExpressionException("No duplicated keys are allowed");
                    }
                    previousEnd = localMatcher.end();
                }
                if (previousEnd < queryString.length()) {
                    throw new ExpressionException(errorMsg);
                }
            }

            return resultBuilder.build();
        }

        private static class ParserBuilder<R> {

            private Pattern globalPattern;
            private Supplier<ParseResultBuilder<R>> resultBuilder;
            private String errorMsg;

            ParserBuilder<R> withResultBuilder(Supplier<ParseResultBuilder<R>> resultBuilder) {
                this.resultBuilder = resultBuilder;
                return this;
            }

            ParserBuilder<R> withGlobalPattern(Pattern globalPattern) {
                this.globalPattern = globalPattern;
                return this;
            }

            ParserBuilder<R> withErrorMsg(String errorMsg) {
                this.errorMsg = errorMsg;
                return this;
            }

            Parser<R> build() {
                return new Parser<>(this);
            }
        }
    }

    private static final class TemplateBuilder
            implements Parser.ParseResultBuilder<TemplateExpression> {

        private final Map<String, ExtractionExpression> params = new HashMap<>();
        private final ExtractionExpressionParser extractionExpressionParser;
        private String prefix;

        private TemplateBuilder(ExtractionExpressionParser extractionExpressionParser) {
            this.extractionExpressionParser = extractionExpressionParser;
        }

        @Override
        public void notifyPrefix(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public boolean notifyParam(String key, String value) {
            if (params.containsKey(key)) {
                return false;
            }
            params.put(key, extractionExpressionParser.parse(value));
            return true;
        }

        @Override
        public TemplateExpression build() {
            return new TemplateExpression(prefix, params);
        }
    }

    private abstract static sealed class AbstractSubscriptionBuilder<S>
            implements Parser.ParseResultBuilder<S>
            permits SubscriptionBuilder, CanonicalItemNameBuilder {

        protected final TreeSet<Data> dataSet = new TreeSet<>();
        protected String prefix;

        @Override
        public final void notifyPrefix(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public final boolean notifyParam(String key, String value) {
            return dataSet.add(Data.from(key, value));
        }
    }

    private static final class SubscriptionBuilder
            extends AbstractSubscriptionBuilder<SubscriptionExpression> {

        @Override
        public SubscriptionExpression build() {
            return new SubscriptionExpression(prefix, dataSet);
        }
    }

    private static final class CanonicalItemNameBuilder
            extends AbstractSubscriptionBuilder<String> {

        @Override
        public String build() {
            return Data.buildItemName(dataSet.toArray(new Data[0]), prefix);
        }
    }

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

    static class ExtractionExpressionParser {

        private static final Pattern FIELD = Pattern.compile(SELECTION_REGEX);

        ExtractionExpression parseWrapped(String wrappedExpression) {
            Matcher matcher = FIELD.matcher(nonNullString(wrappedExpression));
            if (!matcher.matches()) {
                throw new ExpressionException("Invalid expression");
            }
            return parse(matcher.group(1));
        }

        ExtractionExpression parse(String expression) throws ExpressionException {
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
    }

    // ================================
    // STATIC CONSTANTS
    // ================================

    private static final String SELECTION_REGEX = "\\#\\{(.+)\\}";
    private static final String INDEX_REGEX = "\\[(.*)\\]";
    private static final Pattern SUBSCRIPTION_GLOBAL_PATTERN =
            Pattern.compile("([a-zA-Z0-9_-]+)(-" + INDEX_REGEX + ")?");

    private static final Expressions EXPRESSIONS = new Expressions();

    // ================================
    // INSTANCE FIELDS
    // ================================

    private final ExtractionExpressionParser extractionExpressionParser =
            new ExtractionExpressionParser();

    private final Parser<TemplateExpression> templateParser =
            new Parser.ParserBuilder<TemplateExpression>()
                    .withGlobalPattern(
                            Pattern.compile("(^[a-zA-Z0-9_-]+)(-" + SELECTION_REGEX + ")$"))
                    .withResultBuilder(() -> new TemplateBuilder(extractionExpressionParser))
                    .withErrorMsg("Invalid template expression")
                    .build();

    private final Parser<SubscriptionExpression> subscriptionParser =
            new Parser.ParserBuilder<SubscriptionExpression>()
                    .withGlobalPattern(SUBSCRIPTION_GLOBAL_PATTERN)
                    .withResultBuilder(SubscriptionBuilder::new)
                    .withErrorMsg("Invalid Item")
                    .build();

    private final Parser<String> canonicalItemNameParser =
            new Parser.ParserBuilder<String>()
                    .withGlobalPattern(SUBSCRIPTION_GLOBAL_PATTERN)
                    .withResultBuilder(CanonicalItemNameBuilder::new)
                    .withErrorMsg("Invalid Item")
                    .build();

    // ================================
    // CONSTRUCTORS
    // ================================

    private Expressions() {}

    // ================================
    // PUBLIC STATIC FACTORY METHODS
    // ================================

    public static TemplateExpression Template(String templateExpression) {
        return EXPRESSIONS.parseTemplate(templateExpression);
    }

    public static ExtractionExpression Wrapped(String wrappedExpression) {
        return EXPRESSIONS.parseWrapped(wrappedExpression);
    }

    public static SubscriptionExpression Subscription(String subscriptionExpression) {
        return EXPRESSIONS.parseSubscription(subscriptionExpression);
    }

    public static String CanonicalItemName(String expression) {
        return EXPRESSIONS.canonicalItemName(expression);
    }

    public static ExtractionExpression Expression(String expression) {
        return EXPRESSIONS.parseExtractionExpression(expression);
    }

    // ================================
    // PACKAGE-PRIVATE INSTANCE METHODS
    // ================================

    TemplateExpression parseTemplate(String templateExpression) {
        return templateParser.parse(templateExpression);
    }

    SubscriptionExpression parseSubscription(String subscriptionExpression) {
        return subscriptionParser.parse(subscriptionExpression);
    }

    String canonicalItemName(String expression) {
        return canonicalItemNameParser.parse(expression);
    }

    ExtractionExpression parseWrapped(String wrappedExpression) {
        return extractionExpressionParser.parseWrapped(wrappedExpression);
    }

    ExtractionExpression parseExtractionExpression(String expression) {
        return extractionExpressionParser.parse(expression);
    }

    // ================================
    // PRIVATE STATIC UTILITY METHODS
    // ================================

    private static String[] getTokens(String expression) {
        // expression.splitWithDelimiters("\\.", 0); // Valid for Java 21
        StringTokenizer st = new StringTokenizer(expression, ".", true);
        String[] tokens = new String[st.countTokens()];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = st.nextToken();
        }
        return tokens;

        // Split on dot, keeping empty tokens if there are consecutive dots
        // return expression.split("\\.");
    }

    static String nonNullString(String str) {
        return Objects.toString(str, "");
    }
}
