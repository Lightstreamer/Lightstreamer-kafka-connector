
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

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.lightstreamer.kafka.common.expressions.Expressions.ExtractionExpression;
import com.lightstreamer.kafka.common.expressions.Expressions.SubscriptionExpression;
import com.lightstreamer.kafka.common.expressions.Expressions.TemplateExpression;

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
        ExtractionExpression ee1 = Expressions.expression("VALUE");
        ExtractionExpression ee2 = Expressions.expression("VALUE");

        assertThat(ee1.equals(ee1)).isTrue(); // Enforces code coverage
        assertThat(ee1).isEqualTo(ee2);
        assertThat(ee1).isNotSameInstanceAs(ee2);
        assertThat(ee1).isNotEqualTo(new Object());
        assertThat(ee1.hashCode()).isEqualTo(ee2.hashCode());

        ExtractionExpression ee3 = Expressions.expression("KEY");
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
                arguments("VALUE.attrib", Constant.VALUE, Arrays.asList("VALUE", ".", "attrib")));
    }

    @ParameterizedTest
    @MethodSource("expressionArgs")
    void shouldCreateExpression(
            String expressionStr, Constant expectedRoot, List<String> expectedTokens) {
        ExtractionExpression ee = Expressions.expression(expressionStr);
        assertThat(ee.expression()).isEqualTo(expressionStr);
        assertThat(ee.root()).isEqualTo(expectedRoot);
        assertThat(ee.tokens()).asList().isEqualTo(expectedTokens);
    }

    @ParameterizedTest
    @EmptySource
    @NullSource
    @ValueSource(strings = {"NOT-EXISTING-CONSTANT", "..", "@", "\\"})
    void shouldNotCreateExpression(String expressionStr) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class, () -> Expressions.expression(expressionStr));
        assertThat(ee.getMessage())
                .isEqualTo("Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"VALUE.", "VALUE..", "VALUE.attrib.", "VALUE.attrib[]]."})
    void shouldNotCreateExpressionDueToTrailingDots(String expressionStr) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class, () -> Expressions.expression(expressionStr));
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
                        Map.of("param1", "OFFSET", "param2", "PARTITION", "param3", "TIMESTAMP")));
    }

    @ParameterizedTest
    @MethodSource("templateArgs")
    void shouldCreateTemplateExpression(
            String expressionStr, String expectedPrefix, Map<String, String> expectedParams) {
        TemplateExpression template = Expressions.template(expressionStr);
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
                        template-#{name=FOO}                $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                        template-#{name=VALUE,name=OFFSET}  $ No duplicated keys are allowed
                    """)
    void shouldNotCreateTemplateExpression(String expressionStr, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expressions.template(expressionStr));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> subscriptionArgs() {
        return Stream.of(
                arguments("item-prefix-[param=a]", "item-prefix", Map.of("param", "a")),
                arguments(
                        "item-[param1=a,param2=b,param3=c]",
                        "item",
                        Map.of("param1", "a", "param2", "b", "param3", "c")),
                arguments("item-prefix-[param=a,]", "item-prefix", Map.of("param", "a")),
                arguments("item-prefix-[param=a]]", "item-prefix", Map.of("param", "a]")),
                arguments("item-prefix-", "item-prefix-", Collections.emptyMap()),
                arguments("item-prefix-[]", "item-prefix", Collections.emptyMap()));
    }

    @ParameterizedTest
    @MethodSource("subscriptionArgs")
    void shouldCreateSubscriptionEpression(
            String expressionStr, String expectedPrefix, Map<String, String> expectedParams) {
        SubscriptionExpression subscription = Expressions.subscription(expressionStr);
        assertThat(subscription.prefix()).isEqualTo(expectedPrefix);

        Map<String, String> subscriptionParams = subscription.params();
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
    void shouldNotCreateSubscriptionExpression(String expressionStr, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(
                        ExpressionException.class, () -> Expressions.subscription(expressionStr));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }

    static Stream<Arguments> fieldArgs() {
        return Stream.of(
                arguments("#{VALUE}", Constant.VALUE, Arrays.asList("VALUE")),
                arguments("#{OFFSET}", Constant.OFFSET, Arrays.asList("OFFSET")),
                arguments("#{TIMESTAMP}", Constant.TIMESTAMP, Arrays.asList("TIMESTAMP")),
                arguments("#{PARTITION}", Constant.PARTITION, Arrays.asList("PARTITION")),
                arguments("#{KEY}", Constant.KEY, Arrays.asList("KEY")),
                arguments(
                        "#{VALUE.attrib}", Constant.VALUE, Arrays.asList("VALUE", ".", "attrib")));
    }

    @ParameterizedTest
    @MethodSource("fieldArgs")
    void shouldCreateFieldExpression(
            String expressionStr, Constant expectedRoot, List<String> expectedTokens) {
        ExtractionExpression ee = Expressions.field(expressionStr);
        // Remove '#{' and '}'
        String expectedExpression =
                expressionStr.substring(
                        expressionStr.indexOf("#{") + 2, expressionStr.lastIndexOf("}"));
        assertThat(ee.expression()).isEqualTo(expectedExpression);
        assertThat(ee.root()).isEqualTo(expectedRoot);
        assertThat(ee.tokens()).asList().isEqualTo(expectedTokens);
    }

    @ParameterizedTest
    @ValueSource(strings = {"#{NOT-EXISTING-CONSTANT}", "#{..}", "#{@}", "#{\\}"})
    void shouldNotCreateFieldExpression(String expressionStr) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expressions.field(expressionStr));
        assertThat(ee.getMessage())
                .isEqualTo("Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]");
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '$',
            textBlock =
                    """
                        EXPRESSION               $ EXPECTED_ERROR_MESSAGE
                                                 $ Invalid field expression
                        ''                       $ Invalid field expression
                        #{NOT-EXISTING-CONSTANT} $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                        #{..}                    $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                        #{@}                     $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                        #{\\}                    $ Missing root tokens [KEY|VALUE|TIMESTAMP|PARTITION|OFFSET|TOPIC]
                    """)
    void shouldNotCreateFieldExpressionDueToInvalidExpression(
            String expressionStr, String expectedErrorMessage) {
        ExpressionException ee =
                assertThrows(ExpressionException.class, () -> Expressions.field(expressionStr));
        assertThat(ee.getMessage()).isEqualTo(expectedErrorMessage);
    }
}
