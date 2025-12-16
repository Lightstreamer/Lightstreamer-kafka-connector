
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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
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

        default boolean isWildCardExpression() {
            return false;
        }
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

    public record TemplateExpression(String prefix, Map<String, ExtractionExpression> params) {

        public Schema schema() {
            return Schema.from(prefix, params.keySet());
        }
    }

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
                    "Missing root tokens [%s] in the expression [%s]"
                            .formatted(Constant.VALUES_STR, expression));
        }

        public static ExpressionException unexpectedTrailingDots(String expression)
                throws ExpressionException {
            return new ExpressionException(
                    "Found unexpected trailing dot(s) in the expression [%s]"
                            .formatted(expression));
        }

        public static ExpressionException unexpectedWhiteChar(String expression)
                throws ExpressionException {
            return new ExpressionException(
                    "Found unexpected white char in the expression [%s]".formatted(expression));
        }

        public static ExpressionException unexpectedWildCard(String expression)
                throws ExpressionException {
            return new ExpressionException(
                    "Found unexpected wildcard char in the expression [%s]".formatted(expression));
        }

        public static ExpressionException expectedWildCard(String expression)
                throws ExpressionException {
            return new ExpressionException(
                    "Missing wildcard char in the expression [%s]".formatted(expression));
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

        R parse(String expression) throws ExpressionException {
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

        private final SortedMap<String, ExtractionExpression> params = new TreeMap<>();
        private final ExpressionParser expressionParser;
        private String prefix;

        private TemplateBuilder(ExpressionParser expressionParser) {
            this.expressionParser = expressionParser;
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
            ExtractionExpression value2 = expressionParser.parse(value);
            ExpressionParser.checkWildcard(value2, false);
            params.put(key, value2);
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
        private final boolean wildCardExpression;

        private ExtractionExpressionImpl(
                Constant root, String[] tokens, String expression, boolean isWildCard) {
            this.root = root;
            this.completeExpression = expression;
            if (isWildCard) {
                this.tokens = Arrays.copyOf(tokens, tokens.length - 2);
            } else {
                this.tokens = Arrays.copyOf(tokens, tokens.length);
            }
            this.wildCardExpression = isWildCard;
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

        @Override
        public String[] tokens() {
            return Arrays.copyOf(tokens, tokens.length);
        }

        @Override
        public boolean isWildCardExpression() {
            return wildCardExpression;
        }

        @Override
        public int hashCode() {
            return Objects.hash(completeExpression, wildCardExpression);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            return obj instanceof ExtractionExpressionImpl other
                    && Objects.equals(completeExpression, other.completeExpression)
                    && wildCardExpression == other.wildCardExpression;
        }
    }

    private static class ExpressionParser {

        private final Pattern SELECTION = Pattern.compile(SELECTION_REGEX);

        private ExpressionParser() {}

        ExtractionExpression parseWrapped(String wrappedExpression) throws ExpressionException {
            Matcher matcher = SELECTION.matcher(nonNullString(wrappedExpression));
            if (!matcher.matches()) {
                throw new ExpressionException("Invalid expression");
            }
            return parse(matcher.group(1));
        }

        ExtractionExpression parseWrapped(String wrappedExpression, boolean expectedAsWildCard)
                throws ExpressionException {
            ExtractionExpression expression = parseWrapped(wrappedExpression);
            checkWildcard(expression, expectedAsWildCard);
            return expression;
        }

        ExtractionExpression parse(String expression) throws ExpressionException {
            // Here expression is guaranteed to be non-null and non-empty, therefore at least one
            // token is expected
            String[] tokens = getTokens(nonNullString(expression));
            int length = tokens.length;

            Constant root = Constant.from(tokens[0]);
            if (root == null) {
                throw ExpressionException.missingRootTokens(expression);
            }

            String lastToken = tokens[length - 1];
            if (lastToken.equals(".")) {
                throw ExpressionException.unexpectedTrailingDots(expression);
            }

            boolean isWildCard =
                    length >= 2 && lastToken.equals("*") && tokens[length - 2].equals(".");

            return new ExtractionExpressionImpl(root, tokens, expression, isWildCard);
        }

        static void checkWildcard(
                ExtractionExpression extractionExpression, boolean expectedAsWildCard) {
            if (extractionExpression.isWildCardExpression() != expectedAsWildCard) {
                if (expectedAsWildCard) {
                    throw ExpressionException.expectedWildCard(extractionExpression.expression());
                } else {
                    throw ExpressionException.unexpectedWildCard(extractionExpression.expression());
                }
            }
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

    private final ExpressionParser expressionParser = new ExpressionParser();

    private final Parser<TemplateExpression> templateParser =
            new Parser.ParserBuilder<TemplateExpression>()
                    .withGlobalPattern(
                            Pattern.compile("(^[a-zA-Z0-9_-]+)(-" + SELECTION_REGEX + ")$"))
                    .withResultBuilder(() -> new TemplateBuilder(expressionParser))
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

    public static TemplateExpression EmptyTemplate(String item) {
        if (item == null || item.isBlank()) {
            throw new ExpressionException("Invalid Item");
        }
        return new TemplateExpression(item, Collections.emptyMap());
    }

    public static TemplateExpression Template(String templateExpression)
            throws ExpressionException {
        return EXPRESSIONS.parseTemplate(templateExpression);
    }

    public static ExtractionExpression Wrapped(String wrappedExpression)
            throws ExpressionException {
        return EXPRESSIONS.parseWrapped(wrappedExpression);
    }

    public static ExtractionExpression WrappedWithWildcards(String wrappedExpression)
            throws ExpressionException {
        return EXPRESSIONS.parseWrappedWithWildcard(wrappedExpression);
    }

    public static ExtractionExpression WrappedNoWildcardCheck(String wrappedExpression)
            throws ExpressionException {
        return EXPRESSIONS.parseWrappedNoWildcardCheck(wrappedExpression);
    }

    public static SubscriptionExpression Subscription(String subscriptionExpression)
            throws ExpressionException {
        return EXPRESSIONS.parseSubscription(subscriptionExpression);
    }

    public static String CanonicalItemName(String expression) throws ExpressionException {
        return EXPRESSIONS.canonicalItemName(expression);
    }

    public static ExtractionExpression Wrap(String expression) throws ExpressionException {
        return EXPRESSIONS.parseWrapped("#{" + expression + "}");
    }

    public static ExtractionExpression WrapWithWildcards(String expression)
            throws ExpressionException {
        return EXPRESSIONS.parseWrappedWithWildcard("#{" + expression + "}");
    }

    // ================================
    // PACKAGE-PRIVATE INSTANCE METHODS
    // ================================

    TemplateExpression parseTemplate(String templateExpression) throws ExpressionException {
        return templateParser.parse(templateExpression);
    }

    SubscriptionExpression parseSubscription(String subscriptionExpression)
            throws ExpressionException {
        return subscriptionParser.parse(subscriptionExpression);
    }

    String canonicalItemName(String expression) throws ExpressionException {
        return canonicalItemNameParser.parse(expression);
    }

    ExtractionExpression parseWrappedNoWildcardCheck(String fieldExpression)
            throws ExpressionException {
        return expressionParser.parseWrapped(fieldExpression);
    }

    ExtractionExpression parseWrapped(String fieldExpression) throws ExpressionException {
        return expressionParser.parseWrapped(fieldExpression, false);
    }

    ExtractionExpression parseWrappedWithWildcard(String fieldExpression)
            throws ExpressionException {
        return expressionParser.parseWrapped(fieldExpression, true);
    }

    // ================================
    // PRIVATE STATIC UTILITY METHODS
    // ================================

    private static String[] getTokens(String expression) {
        // expression.splitWithDelimiters("\\.", 0); // Valid for Java 21
        StringTokenizer tokenizer = new StringTokenizer(expression, ".", true);
        int tokenCount = tokenizer.countTokens();
        String[] tokens = new String[tokenCount];

        for (int i = 0; i < tokenCount; i++) {
            String token = tokenizer.nextToken();
            if (!token.equals(token.strip())) {
                throw ExpressionException.unexpectedWhiteChar(expression);
            }
            tokens[i] = token;
        }
        return tokens;

        // Split on dot, keeping empty tokens if there are consecutive dots
        // return expression.split("\\.");
    }

    static String nonNullString(String str) {
        return Objects.toString(str, "");
    }
}
