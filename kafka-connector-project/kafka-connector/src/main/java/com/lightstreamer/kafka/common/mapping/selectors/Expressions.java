
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

import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
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
        KEY,
        VALUE,
        TIMESTAMP,
        PARTITION,
        OFFSET,
        TOPIC,
        HEADERS(true);

        private static final Map<String, Constant> NAME_CACHE;

        static {
            NAME_CACHE =
                    Stream.of(values()).collect(toMap(Constant::toString, Function.identity()));
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
            if (constant != null) {
                if (!constant.allowIndex() && indexed) {
                    constant = null;
                }
            }
            return constant;
        }

        static Set<Constant> all() {
            return Arrays.stream(values()).collect(Collectors.toCollection(LinkedHashSet::new));
        }

        private final String tokens[] = new String[1];
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

    private abstract static sealed class Parser<R, M>
            permits SubscriptionParser, TemplateParser, SimpleSubscriptionParser {

        private final String errorMsg;

        Parser(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        R parse(String expression) {
            return parse(expression, HashMap::new, this::build);
        }

        R parse(
                String expression,
                Supplier<Map<String, M>> paramsSupplier,
                BiFunction<String, Map<String, M>, R> builder) {
            Pattern globalPattern = globalPattern();
            Matcher globalMatcher = globalPattern.matcher(nonNullString(expression));
            if (!globalMatcher.matches()) {
                throw new ExpressionException(errorMsg);
            }

            String prefix = globalMatcher.group(1);
            Map<String, M> bindParams = paramsSupplier.get();
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
                    if (bindParams.put(key, remapValue(value)) != null) {
                        throw new ExpressionException("No duplicated keys are allowed");
                    }
                    previousEnd = localMatcher.end();
                }
                if (previousEnd < queryString.length()) {
                    throw new ExpressionException(errorMsg);
                }
            }

            return builder.apply(prefix, bindParams);
        }

        abstract Pattern globalPattern();

        abstract M remapValue(String value);

        abstract R build(String prefix, Map<String, M> params);
    }

    private static final class SubscriptionParser extends Parser<SchemaAndValues, String> {

        private final Pattern subscribedGlobal =
                Pattern.compile("([a-zA-Z0-9_-]+)(-" + INDEX_REGEX + ")?");

        private SubscriptionParser() {
            super("Invalid Item");
        }

        @Override
        Pattern globalPattern() {
            return subscribedGlobal;
        }

        @Override
        String remapValue(String value) {
            return value;
        }

        @Override
        SchemaAndValues build(String prefix, Map<String, String> params) {
            return SchemaAndValues.from(prefix, params);
        }
    }

    private static final class SimpleSubscriptionParser extends Parser<String, String> {

        private final Pattern normalizerGlobal =
                Pattern.compile("([a-zA-Z0-9_-]+)(-" + INDEX_REGEX + ")?");

        private SimpleSubscriptionParser() {
            super("Invalid expression");
        }

        @Override
        Pattern globalPattern() {
            return normalizerGlobal;
        }

        @Override
        String remapValue(String value) {
            return value;
        }

        @Override
        String build(String prefix, Map<String, String> params) {
            return SchemaAndValues.formatWithArraySort(prefix, params);
        }
    }

    private static final class TemplateParser
            extends Parser<TemplateExpression, ExtractionExpression> {

        private final Pattern templateGlobal =
                Pattern.compile("(^[a-zA-Z0-9_-]+)(-" + SELECTION_REGEX + ")$");

        private final ExtractionExpressionParser extractionExpressionParser;

        private TemplateParser(ExtractionExpressionParser extractionExpressionParser) {
            super("Invalid template expression");
            this.extractionExpressionParser = extractionExpressionParser;
        }

        @Override
        Pattern globalPattern() {
            return templateGlobal;
        }

        @Override
        ExtractionExpression remapValue(String value) {
            return extractionExpressionParser.parse(value);
        }

        @Override
        TemplateExpression build(String prefix, Map<String, ExtractionExpression> params) {
            return new TemplateExpression(prefix, params);
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

    private static final Pattern PARAM = Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?");
    private static final Pattern FIELD = Pattern.compile(SELECTION_REGEX);

    private static final Expressions EXPRESSIONS = new Expressions();

    // ================================
    // INSTANCE FIELDS
    // ================================

    // Parsers
    private final SubscriptionParser subscriptionParser = new SubscriptionParser();

    private final ExtractionExpressionParser extractionExpressionParser =
            new ExtractionExpressionParser();

    private final TemplateParser templateParser = new TemplateParser(extractionExpressionParser);

    private final SimpleSubscriptionParser simpleSubscriptionParser =
            new SimpleSubscriptionParser();

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

    public static SchemaAndValues Subscription(String subscriptionExpression) {
        return EXPRESSIONS.parseSubscription(subscriptionExpression);
    }

    /**
     * Normalizes the given subscription expression to a canonical string format.
     *
     * <p>This method takes a subscription expression that may contain parameters in square brackets
     * and normalizes it by:
     *
     * <ul>
     *   <li>Parsing the expression into a prefix and parameter key-value pairs
     *   <li>Sorting the parameters alphabetically by key
     *   <li>Formatting the result in a consistent canonical form
     * </ul>
     *
     * <h3>Expression Format</h3>
     *
     * The input expression follows the pattern: {@code prefix-[key1=value1,key2=value2,...]}
     *
     * <ul>
     *   <li><strong>prefix</strong>: A valid identifier containing alphanumeric characters,
     *       underscores, or hyphens
     *   <li><strong>parameters</strong>: Optional comma-separated key-value pairs enclosed in
     *       square brackets
     * </ul>
     *
     * <h3>Normalization Process</h3>
     *
     * <ol>
     *   <li>Extract the prefix and parameters from the input expression
     *   <li>Parse parameters into key-value pairs
     *   <li>Sort parameters alphabetically by key for consistent ordering
     *   <li>Reconstruct the expression with sorted parameters
     * </ol>
     *
     * <h3>Examples</h3>
     *
     * <pre>{@code
     * // Simple expression without parameters
     * Expressions.Normalize("item1")
     * // Returns: "item1"
     *
     * // Expression with parameters (already sorted)
     * Expressions.Normalize("item1-[field1=value1,field2=value2]")
     * // Returns: "item1-[field1=value1,field2=value2]"
     *
     * // Expression with unsorted parameters (gets normalized)
     * Expressions.Normalize("item1-[zebra=last,alpha=first,beta=middle]")
     * // Returns: "item1-[alpha=first,beta=middle,zebra=last]"
     *
     * // Expression with special characters in values
     * Expressions.Normalize("topic_name-[partition=1,offset=12345]")
     * // Returns: "topic_name-[offset=12345,partition=1]"
     * }</pre>
     *
     * <h3>Use Cases</h3>
     *
     * <ul>
     *   <li><strong>Subscription matching</strong>: Ensures consistent comparison of subscription
     *       expressions
     *   <li><strong>Caching</strong>: Provides stable keys for caching parsed expressions
     *   <li><strong>Routing</strong>: Enables reliable routing based on normalized subscription
     *       identifiers
     *   <li><strong>Deduplication</strong>: Helps identify duplicate subscriptions with different
     *       parameter orders
     * </ul>
     *
     * @param expression the subscription expression string to normalize. Can be a simple identifier
     *     or an identifier with parameters in square brackets
     * @return the normalized expression string with parameters sorted alphabetically by key, or the
     *     original prefix if no parameters are present
     * @throws ExpressionException if the expression format is invalid or contains duplicate
     *     parameter keys
     * @see #Subscription(String) for parsing subscription expressions into structured objects
     */
    public static String Normalize(String expression) {
        return Normalize(expression, TreeMap::new, SchemaAndValues::formatWithAlreadySorted);
    }

    static String Normalize(
            String expression,
            Supplier<Map<String, String>> defaultParamsSupplier,
            BiFunction<String, Map<String, String>, String> formatter) {
        return EXPRESSIONS.normalizeSubscription(expression, defaultParamsSupplier, formatter);
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

    SchemaAndValues parseSubscription(String subscriptionExpression) {
        return subscriptionParser.parse(subscriptionExpression);
    }

    String normalizeSubscription(
            String expression,
            Supplier<Map<String, String>> defaultParamsSupplier,
            BiFunction<String, Map<String, String>, String> formatter) {
        return simpleSubscriptionParser.parse(expression, defaultParamsSupplier, formatter);
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
        //  return expression.split("\\.");
    }

    static String nonNullString(String str) {
        return Objects.toString(str, "");
    }
}
