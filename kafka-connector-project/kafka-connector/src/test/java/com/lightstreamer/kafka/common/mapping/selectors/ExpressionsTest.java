
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

import static com.google.common.truth.Truth.assertThat;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Subscription;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Template;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ExpressionsTest {

    @Test
    void shouldCreateEqualExpressions() {
        ExtractionExpression ee1 = Expression("VALUE");
        ExtractionExpression ee2 = Expression("VALUE");

        assertThat(ee1.equals(ee1)).isTrue(); // Enforces code coverage
        assertThat(ee1).isEqualTo(ee2);
        assertThat(ee1).isNotSameInstanceAs(ee2);
        assertThat(ee1).isNotEqualTo(new Object());
        assertThat(ee1.hashCode()).isEqualTo(ee2.hashCode());

        ExtractionExpression ee3 = Expression("KEY");
        assertThat(ee1).isNotEqualTo(ee3);
        assertThat(ee1.hashCode()).isNotEqualTo(ee3.hashCode());
    }

    static Stream<Arguments> expressionArgs() {
        return Stream.of(
                arguments("VALUE", Constant.VALUE, Arrays.asList("VALUE")),
                arguments("OFFSET", Constant.OFFSET, Arrays.asList("OFFSET")),
                arguments("TIMESTAMP", Constant.TIMESTAMP, Arrays.asList("TIMESTAMP")),
                arguments("PARTITION", Constant.PARTITION, Arrays.asList("PARTITION")),
                arguments("KEY", Constant.KEY, Arrays.asList("KEY")),
                arguments("VALUE.attrib", Constant.VALUE, Arrays.asList("VALUE", ".", "attrib")),
                arguments(
                        "VALUE.attrib[]", Constant.VALUE, Arrays.asList("VALUE", ".", "attrib[]")),
                arguments(
                        "HEADERS['attrib']", Constant.HEADERS, Arrays.asList("HEADERS['attrib']")));
    }

    @ParameterizedTest
    @MethodSource("expressionArgs")
    void shouldParseExtractionExpression(
            String expressionStr, Constant expectedRoot, List<String> expectedTokens) {
        ExtractionExpression ee = Expression(expressionStr);
        assertThat(ee.expression()).isEqualTo(expressionStr);
        assertThat(ee.constant()).isEqualTo(expectedRoot);
        assertThat(ee.tokens()).asList().isEqualTo(expectedTokens);
    }

    @ParameterizedTest
    @EmptySource
    @NullSource
    @ValueSource(strings = {"NOT-EXISTING-CONSTANT", "..", "@", "\\"})
    void shouldNotParseExpression(String expressionStr) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expression(expressionStr));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE.", "VALUE..", "VALUE.attrib.", "VALUE.attrib[]]."})
    void shouldNotParseExpressionDueToTrailingDots(String expressionStr) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expression(expressionStr));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Found unexpected trailing dot(s) in the expression ["
                                + expressionStr
                                + "]");
    }

    static Stream<Arguments> templateArgs() {
        return Stream.of(
                arguments(
                        "template-prefix-#{param=VALUE}",
                        "template-prefix",
                        Map.of("param", "VALUE")),
                arguments(
                        "template-#{param1=OFFSET,param2=PARTITION,param3=TIMESTAMP}",
                        "template",
                        Map.of("param1", "OFFSET", "param2", "PARTITION", "param3", "TIMESTAMP")),
                arguments(
                        "template-#{param1=VALUE.complex_attrib_name.child_1_}",
                        "template",
                        Map.of("param1", "VALUE.complex_attrib_name.child_1_")),
                arguments(
                        "indexed-template-#{param=VALUE.attrib[0]}",
                        "indexed-template",
                        Map.of("param", "VALUE.attrib[0]")),
                arguments(
                        "indexed-template-#{param=VALUE.attrib[''key'][}",
                        "indexed-template",
                        Map.of("param", "VALUE.attrib[''key'][")));
    }

    @ParameterizedTest
    @MethodSource("templateArgs")
    void shouldParseTemplateExpression(
            String expressionStr, String expectedPrefix, Map<String, String> expectedParams) {
        TemplateExpression template = Template(expressionStr);
        assertThat(template.prefix()).isEqualTo(expectedPrefix);

        Map<String, ExtractionExpression> templateParams = template.params();
        assertThat(templateParams.keySet()).isEqualTo(expectedParams.keySet());

        for (Map.Entry<String, String> expectedParam : expectedParams.entrySet()) {
            ExtractionExpression ee = templateParams.get(expectedParam.getKey());
            assertThat(ee.toString()).isEqualTo(expectedParam.getValue());
        }
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '$',
            textBlock =
                    """
                EXPRESSION                          $ EXPECTED_ERROR_MESSAGE
                                                    $ Invalid template expression
                ''                                  $ Invalid template expression
                -                                   $ Invalid template expression
                \\                                  $ Invalid template expression
                @                                   $ Invalid template expression
                !                                   $ Invalid template expression
                |                                   $ Invalid template expression
                item-first                          $ Invalid template expression
                item_123_                           $ Invalid template expression
                item!                               $ Invalid template expression
                item@                               $ Invalid template expression
                item\\                              $ Invalid template expression
                item-                               $ Invalid template expression
                prefix-#                            $ Invalid template expression
                prefix-#{}                          $ Invalid template expression
                template-#{name=FOO}                $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]
                template-#{name=VALUE,name=OFFSET}  $ No duplicated keys are allowed
                template-#{name=VALUE,par}          $ Invalid template expression
                template-#{name=VALUE.}             $ Found unexpected trailing dot(s) in the expression [VALUE.]
                template-#{name=VALUE[1]}           $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]
                    """)
    void shouldNotParseTemplateExpression(String expressionStr, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Template(expressionStr));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> subscriptionArgs() {
        return Stream.of(
                arguments("item", "item", Collections.emptyMap()),
                arguments("item-prefix-[param=a]", "item-prefix", Map.of("param", "a")),
                arguments(
                        "item-[param1=a,param2=b,param3=c]",
                        "item",
                        Map.of("param1", "a", "param2", "b", "param3", "c")),
                arguments("item-prefix-[param=a,]", "item-prefix", Map.of("param", "a")),
                arguments("item-prefix-[param=a.b]", "item-prefix", Map.of("param", "a.b")),
                arguments("item-prefix-[param=a]]", "item-prefix", Map.of("param", "a]")),
                // arguments("item-prefix-[param1=a param with spaces]", "item-prefix",
                // Map.of("param1", "a param with spaces")),
                arguments(
                        "item-prefix-[param1=a param with spaces,param2=another param with values]",
                        "item-prefix",
                        Map.of(
                                "param1",
                                "a param with spaces",
                                "param2",
                                "another param with values")),
                arguments(
                        "item-[param1=a[0],param2=b[1],param3=c[2]]",
                        "item",
                        Map.of("param1", "a[0]", "param2", "b[1]", "param3", "c[2]")),
                arguments("item-prefix-", "item-prefix-", Collections.emptyMap()),
                arguments("item-prefix-[]", "item-prefix", Collections.emptyMap()));
    }

    @ParameterizedTest
    @MethodSource("subscriptionArgs")
    void shouldParseSubscriptionExpression(
            String expressionStr, String expectedPrefix, Map<String, String> expectedParams) {
        SchemaAndValues subscription = Subscription(expressionStr);
        assertThat(subscription.schema().name()).isEqualTo(expectedPrefix);

        Map<String, String> subscriptionParams = subscription.values();
        assertThat(subscriptionParams.keySet()).isEqualTo(expectedParams.keySet());

        for (Map.Entry<String, String> expectedParam : expectedParams.entrySet()) {
            String paramValue = subscriptionParams.get(expectedParam.getKey());
            assertThat(paramValue).isEqualTo(expectedParam.getValue());
        }
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '$',
            textBlock =
                    """
                EXPRESSION                         $ EXPECTED_ERROR_MESSAGE
                                                   $ Invalid Item
                ''                                 $ Invalid Item
                template-[name=VALUE,name=OFFSET]  $ No duplicated keys are allowed
                    """)
    void shouldNotParseSubscriptionExpression(String expressionStr, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Subscription(expressionStr));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> fieldArgs() {
        return Stream.of(
                arguments("#{VALUE}", Constant.VALUE, Arrays.asList("VALUE")),
                arguments("#{OFFSET}", Constant.OFFSET, Arrays.asList("OFFSET")),
                arguments("#{TIMESTAMP}", Constant.TIMESTAMP, Arrays.asList("TIMESTAMP")),
                arguments("#{PARTITION}", Constant.PARTITION, Arrays.asList("PARTITION")),
                arguments("#{KEY}", Constant.KEY, Arrays.asList("KEY")),
                arguments("#{VALUE.attrib}", Constant.VALUE, Arrays.asList("VALUE", ".", "attrib")),
                arguments("#{HEADERS}", Constant.HEADERS, Arrays.asList("HEADERS")),
                arguments(
                        "#{HEADERS.attrib}",
                        Constant.HEADERS,
                        Arrays.asList("HEADERS", ".", "attrib")),
                arguments(
                        "#{HEADERS['attrib']}",
                        Constant.HEADERS,
                        Arrays.asList("HEADERS['attrib']")));
    }

    @ParameterizedTest
    @MethodSource("fieldArgs")
    void shouldParseWrappedExpression(
            String expressionStr, Constant expectedRoot, List<String> expectedTokens) {
        ExtractionExpression ee = Wrapped(expressionStr);
        // Remove '#{' and '}'
        String expectedExpression =
                expressionStr.substring(
                        expressionStr.indexOf("#{") + 2, expressionStr.lastIndexOf("}"));
        assertThat(ee.expression()).isEqualTo(expectedExpression);
        assertThat(ee.constant()).isEqualTo(expectedRoot);
        assertThat(ee.tokens()).asList().isEqualTo(expectedTokens);
    }

    @ParameterizedTest
    @ValueSource(strings = {"#{NOT-EXISTING-CONSTANT}", "#{..}", "#{@}", "#{\\}"})
    void shouldNotParseWrappedExpression(String expressionStr) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Wrapped(expressionStr));
        assertThat(ee.getMessage())
                .isEqualTo(
                        "Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]");
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '$',
            textBlock =
                    """
                EXPRESSION               $ EXPECTED_ERROR_MESSAGE
                                         $ Invalid expression
                ''                       $ Invalid expression
                #{NOT-EXISTING-CONSTANT} $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                #{..}                    $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                #{@}                     $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                #{\\}                    $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                    """)
    void shouldNotParseWrappedExpressionDueToInvalidExpression(
            String expressionStr, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Wrapped(expressionStr));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '$',
            textBlock =
                    """
                EXPRESSION          $ EXPECTED_NORMALIZATION
                item-[a=1]          $ item-[a=1]
                item-[a=1,b=2]      $ item-[a=1,b=2]
                item-[c=3,a=1,b=2]  $ item-[a=1,b=2,c=3]
                item-[b=2,a=1]      $ item-[a=1,b=2]
                item                $ item
                item-               $ item-
                item-[]             $ item
                    """)
    void shouldNormalizeExpression(String expression, String expectedNormalization) {
        String normalization = Expressions.Normalize(expression);
        assertThat(normalization).isEqualTo(expectedNormalization);
    }
}
