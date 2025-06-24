
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

package com.lightstreamer.kafka.common.mapping;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ItemsTest {

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                INPUT                     | EXPECTED_PREFIX | EXPECTED_NAME | EXPECTED_VALUE
                item-[name=field1]        | item            | name          | field1
                item-first-[height=12.34] | item-first      | height        | 12.34
                item_123_-[test=\\]       | item_123_       | test          | \\
                item-[test=""]            | item            | test          | ""
                prefix-[test=]]           | prefix          | test          | ]
                item-[test=value,]        | item            | test          | value
                item-                     | item-           |               |
                item-[]                   | item            |               |
                item                      | item            |               |
                item-first                | item-first      |               |
                item_123_                 | item_123_       |               |
                    """)
    public void shouldSubscribeFromInputAndHandle(
            String input, String expectedPrefix, String expectedName, String expectedValue) {
        Object handle = new Object();
        SubscribedItem item = Items.subscribedFrom(input, handle);
        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isSameInstanceAs(handle);
        assertThat(item.schema().name()).isEqualTo(expectedPrefix);

        if (expectedName != null && expectedValue != null) {
            assertThat(item.values()).containsExactly(expectedName, expectedValue);
        } else {
            assertThat(item.schema().keys()).isEmpty();
            assertThat(item.values()).isEmpty();
        }
    }

    @ParameterizedTest
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                INPUT                     | EXPECTED_PREFIX | EXPECTED_NAME | EXPECTED_VALUE
                item-[name=field1]        | item            | name          | field1
                item-first-[height=12.34] | item-first      | height        | 12.34
                item_123_-[test=\\]       | item_123_       | test          | \\
                item-[test=""]            | item            | test          | ""
                prefix-[test=]]           | prefix          | test          | ]
                item-[test=value,]        | item            | test          | value
                item-                     | item-           |               |
                item-[]                   | item            |               |
                item                      | item            |               |
                item-first                | item-first      |               |
                item_123_                 | item_123_       |               |
                    """)
    public void shouldSubscribeFromInputOnly(
            String input, String expectedPrefix, String expectedName, String expectedValue) {
        SubscribedItem item = Items.subscribedFrom(input);
        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isEqualTo(input);
        assertThat(item.schema().name()).isEqualTo(expectedPrefix);

        if (expectedName != null && expectedValue != null) {
            assertThat(item.values()).containsExactly(expectedName, expectedValue);
        } else {
            assertThat(item.schema().keys()).isEmpty();
            assertThat(item.values()).isEmpty();
        }
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                INPUT                            | EXPECTED_NAME1 | EXPECTED_VALUE1 | EXPECTED_NAME2 | EXPECTED_VALUE2
                item-[name1=field1,name2=field2] | name1          | field1          | name2          | field2
                    """)
    public void shouldSubscribeInputAndHandleWithMoreValues(
            String input, String name1, String val1, String name2, String value2) {
        Object handle = new Object();
        SubscribedItem item = Items.subscribedFrom(input, handle);

        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isSameInstanceAs(handle);
        assertThat(item.schema().name()).isEqualTo("item");
        assertThat(item.schema().keys()).containsExactly(name1, name2);
        assertThat(item.values()).containsExactly(name1, val1, name2, value2);
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                INPUT                            | EXPECTED_NAME1 | EXPECTED_VALUE1 | EXPECTED_NAME2 | EXPECTED_VALUE2
                item-[name1=field1,name2=field2] | name1          | field1          | name2          | field2
                    """)
    public void shouldSubscribeInputOnlyWithMoreValues(
            String input, String name1, String val1, String name2, String value2) {
        SubscribedItem item = Items.subscribedFrom(input);

        assertThat(item).isNotNull();
        assertThat(item.itemHandle()).isEqualTo(input);
        assertThat(item.schema().name()).isEqualTo("item");
        assertThat(item.schema().keys()).containsExactly(name1, name2);
        assertThat(item.values()).containsExactly(name1, val1, name2, value2);
    }

    @Test
    public void shouldSubscribeMatchableItemsIrrespectiveOfTheParamOrders() {
        SubscribedItem item1 =
                Items.subscribedFrom("prefix-[name1=field1,mame2=field2]", new Object());
        SubscribedItem item2 =
                Items.subscribedFrom("prefix-[mame2=field2,name1=field1]", new Object());
        assertThat(item1.matches(item2)).isTrue();
    }
}
