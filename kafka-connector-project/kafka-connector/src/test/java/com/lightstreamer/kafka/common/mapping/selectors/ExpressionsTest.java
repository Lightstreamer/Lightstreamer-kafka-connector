
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
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.HEADERS;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.KEY;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.OFFSET;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.PARTITION;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.TIMESTAMP;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant.VALUE;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Expression;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Subscription;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Template;
import static com.lightstreamer.kafka.common.mapping.selectors.Expressions.Wrapped;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import static java.util.Collections.emptySet;

import com.lightstreamer.kafka.common.mapping.selectors.Expressions.Constant;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExpressionException;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.mapping.selectors.Expressions.TemplateExpression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
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
                arguments("VALUE", VALUE, Arrays.asList("VALUE")),
                arguments("OFFSET", OFFSET, Arrays.asList("OFFSET")),
                arguments("TIMESTAMP", TIMESTAMP, Arrays.asList("TIMESTAMP")),
                arguments("PARTITION", PARTITION, Arrays.asList("PARTITION")),
                arguments("KEY", KEY, Arrays.asList("KEY")),
                arguments("VALUE.attrib", VALUE, Arrays.asList("VALUE", ".", "attrib")),
                arguments("VALUE.*", VALUE, Arrays.asList("VALUE", ".", "*")),
                arguments("VALUE.attrib[]", VALUE, Arrays.asList("VALUE", ".", "attrib[]")),
                arguments("VALUE['name']", VALUE, Arrays.asList("VALUE['name']")),
                arguments("VALUE['name'].aaa", VALUE, Arrays.asList("VALUE['name']", ".", "aaa")),
                arguments("HEADERS['attrib']", HEADERS, Arrays.asList("HEADERS['attrib']")));
    }

    @ParameterizedTest
    @MethodSource("expressionArgs")
    void shouldParseExtractionExpression(
            String expression, Constant expectedRoot, List<String> expectedTokens) {
        ExtractionExpression ee = Expression(expression);
        assertThat(ee.expression()).isEqualTo(expression);
        assertThat(ee.constant()).isEqualTo(expectedRoot);
        assertThat(ee.tokens()).asList().isEqualTo(expectedTokens);
    }

    @ParameterizedTest
    @EmptySource
    @NullSource
    @ValueSource(strings = {"NOT-EXISTING-CONSTANT", "..", "@", "\\", "VALUE OFFSET}"})
    void shouldNotParseExpressionDueToMissingRootTokens(String expression) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expression(expression));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo(
                        "Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE.", "VALUE..", "VALUE.attrib.", "VALUE.attrib[]]."})
    void shouldNotParseExpressionDueToTrailingDots(String expression) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expression(expression));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo(
                        "Found unexpected trailing dot(s) in the expression [" + expression + "]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE. ", "VALUE.a .b", "VALUE. a"})
    void shouldNotParseExpressionDueTokensWithWhiteChars(String expression) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expression(expression));
        assertThat(ee)
                .hasMessageThat()
                .isEqualTo("Found unexpected white char in the expression [" + expression + "]");
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
            String expression, String expectedPrefix, Map<String, String> expectedParams) {
        TemplateExpression template = Template(expression);
        assertThat(template.prefix()).isEqualTo(expectedPrefix);

        Map<String, ExtractionExpression> templateParams = template.params();
        assertThat(templateParams.keySet()).isEqualTo(expectedParams.keySet());

        for (Map.Entry<String, String> expectedParam : expectedParams.entrySet()) {
            ExtractionExpression ee = templateParams.get(expectedParam.getKey());
            assertThat(ee.toString()).isEqualTo(expectedParam.getValue());
        }
    }

    @Test
    void shouldParseEqualsTemplateExpression() {
        TemplateExpression t1 = Template("template-#{param1=VALUE,param2=OFFSET}");
        TemplateExpression t2 = Template("template-#{param2=OFFSET,param1=VALUE}");

        assertThat(t1.equals(t1)).isTrue(); // Enforces code coverage
        assertThat(t1).isEqualTo(t2);
        assertThat(t1).isNotSameInstanceAs(t2);
        assertThat(t1).isNotEqualTo(new Object());
        assertThat(t1.hashCode()).isEqualTo(t2.hashCode());

        TemplateExpression t3 = Template("template-#{param=KEY}");
        assertThat(t1).isNotEqualTo(t3);
        assertThat(t1.hashCode()).isNotEqualTo(t3.hashCode());
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
                template-#{name=VALUE.a }           $ Found unexpected white char in the expression [VALUE.a ]
                template-#{name=VALUE. a}           $ Found unexpected white char in the expression [VALUE. a]
                template-#{name=VALUE.}             $ Found unexpected trailing dot(s) in the expression [VALUE.]
                template-#{name=VALUE[1]aaa}        $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC|HEADERS]
                    """)
    void shouldNotParseTemplateExpression(String expression, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Template(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
    }

    @Test
    void shouldCreateEmptyTemplate() {
        TemplateExpression template = Expressions.EmptyTemplate("item");
        assertThat(template.prefix()).isEqualTo("item");
        assertThat(template.params()).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   ", "\t", "\n"})
    void shouldNotCreateEmptyTemplate(String invalidItem) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class, () -> Expressions.EmptyTemplate(invalidItem));
        assertThat(ee).hasMessageThat().isEqualTo("Invalid Item");
    }

    static Stream<Arguments> subscriptionArgs() {
        return Stream.of(
                arguments("item", "item", emptySet(), "item"),
                arguments(
                        "item-prefix-[param=a]",
                        "item-prefix",
                        Set.of(Data.from("param", "a")),
                        "item-prefix-[param=a]"),
                arguments(
                        "item-[param2=b,param1=a,param3=c]",
                        "item",
                        Set.of(
                                Data.from("param1", "a"),
                                Data.from("param2", "b"),
                                Data.from("param3", "c")),
                        "item-[param1=a,param2=b,param3=c]"),
                arguments(
                        "item-prefix-[param=a,]",
                        "item-prefix",
                        Set.of(Data.from("param", "a")),
                        "item-prefix-[param=a]"),
                arguments(
                        "item-prefix-[param=a.b]",
                        "item-prefix",
                        Set.of(Data.from("param", "a.b")),
                        "item-prefix-[param=a.b]"),
                arguments(
                        "item-prefix-[param=a]]",
                        "item-prefix",
                        Set.of(Data.from("param", "a]")),
                        "item-prefix-[param=a]]"),
                arguments(
                        "item-prefix-[param1=a param with spaces]",
                        "item-prefix",
                        Set.of(Data.from("param1", "a param with spaces")),
                        "item-prefix-[param1=a param with spaces]"),
                arguments(
                        "item-prefix-[param2=another param with values,param1=a param with spaces]",
                        "item-prefix",
                        Set.of(
                                Data.from("param1", "a param with spaces"),
                                Data.from("param2", "another param with values")),
                        "item-prefix-[param1=a param with spaces,param2=another param with values]"),
                arguments(
                        "item-[param3=c[2],param2=b[1],param1=a[0]]",
                        "item",
                        Set.of(
                                Data.from("param1", "a[0]"),
                                Data.from("param2", "b[1]"),
                                Data.from("param3", "c[2]")),
                        "item-[param1=a[0],param2=b[1],param3=c[2]]"),
                arguments("item-prefix-", "item-prefix-", emptySet(), "item-prefix-"),
                arguments(
                        "item-[other=test,key=value=ue]",
                        "item",
                        Set.of(Data.from("key", "value=ue"), Data.from("other", "test")),
                        "item-[key=value=ue,other=test]"),
                arguments("item-prefix-[]", "item-prefix", emptySet(), "item-prefix"));
    }

    @ParameterizedTest
    @MethodSource("subscriptionArgs")
    void shouldParseSubscriptionExpression(
            String expression,
            String expectedPrefix,
            Set<Data> expectedParams,
            String expectedCanonicalItem) {
        SubscriptionExpression subscription = Subscription(expression);
        assertThat(subscription.prefix()).isEqualTo(expectedPrefix);
        assertThat(subscription.schema().name()).isEqualTo(expectedPrefix);
        assertThat(subscription.asCanonicalItemName()).isEqualTo(expectedCanonicalItem);

        SortedSet<Data> subscriptionParams = subscription.dataSet();
        assertThat(subscriptionParams).isEqualTo(expectedParams);
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
    void shouldNotParseSubscriptionExpression(String expression, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Subscription(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> fieldArgs() {
        return Stream.of(
                arguments("#{VALUE}", VALUE, Arrays.asList("VALUE")),
                arguments("#{OFFSET}", OFFSET, Arrays.asList("OFFSET")),
                arguments("#{TIMESTAMP}", TIMESTAMP, Arrays.asList("TIMESTAMP")),
                arguments("#{PARTITION}", PARTITION, Arrays.asList("PARTITION")),
                arguments("#{KEY}", KEY, Arrays.asList("KEY")),
                arguments("#{VALUE.attrib}", VALUE, Arrays.asList("VALUE", ".", "attrib")),
                arguments("#{HEADERS}", HEADERS, Arrays.asList("HEADERS")),
                arguments("#{HEADERS.attrib}", HEADERS, Arrays.asList("HEADERS", ".", "attrib")),
                arguments("#{HEADERS['attrib']}", HEADERS, Arrays.asList("HEADERS['attrib']")));
    }

    @ParameterizedTest
    @MethodSource("fieldArgs")
    void shouldParseWrappedExpression(
            String expression, Constant expectedRoot, List<String> expectedTokens) {
        ExtractionExpression ee = Wrapped(expression);
        // Remove '#{' and '}'
        String expectedExpression =
                expression.substring(expression.indexOf("#{") + 2, expression.lastIndexOf("}"));
        assertThat(ee.expression()).isEqualTo(expectedExpression);
        assertThat(ee.constant()).isEqualTo(expectedRoot);
        assertThat(ee.tokens()).asList().isEqualTo(expectedTokens);
    }

    @ParameterizedTest
    @ValueSource(strings = {"#{NOT-EXISTING-CONSTANT}", "#{..}", "#{@}", "#{\\}"})
    void shouldNotParseWrappedExpression(String expression) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> Wrapped(expression));
        assertThat(ee)
                .hasMessageThat()
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
            String expression, String expectedErrorMessage) {
        ExpressionException ee = assertThrows(ExpressionException.class, () -> Wrapped(expression));
        assertThat(ee).hasMessageThat().isEqualTo(expectedErrorMessage);
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
                item-[key=val=ue]   $ item-[key=val=ue]
                item-[c=3,a=1,b=2]  $ item-[a=1,b=2,c=3]
                item-[b=2,a=1]      $ item-[a=1,b=2]
                item                $ item
                item-               $ item-
                item-[]             $ item
                    """)
    void shouldGetCanonicalItemFromExpression(String expression, String expectedCanonicalItem) {
        String canonicalItem = Expressions.CanonicalItemName(expression);
        assertThat(canonicalItem).isEqualTo(expectedCanonicalItem);
    }
}
