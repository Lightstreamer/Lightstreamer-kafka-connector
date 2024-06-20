
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

package com.lightstreamer.kafka.connect.config;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class LightstreamerConnectorConfigTest {

    @Test
    void shouldGetOneItemTemplate() {
        Map<String, String> props = new HashMap<>();

        // # item.templates=stock-template:stock-#{index=KEY}
        props.put(LightstreamerConnectorConfig.ITEM_TEMPLATES, "stock-template:stock-#{index=KEY}");

        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        Map<String, String> itemTemplates = config.getItemTemplates();
        assertThat(itemTemplates).containsExactly("stock-template", "stock-#{index=KEY}");
    }

    @Test
    void shouldGetMoreItemTemplates() {
        Map<String, String> props = new HashMap<>();

        props.put(
                LightstreamerConnectorConfig.ITEM_TEMPLATES,
                "stock-template:stock-#{index=KEY};product-template:product-#{id=KEY,price=VALUE.price}");

        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        Map<String, String> itemTemplate = config.getItemTemplates();
        assertThat(itemTemplate)
                .containsExactly(
                        "stock-template",
                        "stock-#{index=KEY}",
                        "product-template",
                        "product-#{id=KEY,price=VALUE.price}");
    }

    @Test
    void shouldGetTopicMappings() {
        Map<String, String> props = new HashMap<>();

        props.put(
                LightstreamerConnectorConfig.TOPIC_MAPPINGS,
                "stocks:item-template.stock-template,stocks:item-template.stock-template-2,orders:item-template.order-template");
        LightstreamerConnectorConfig config = new LightstreamerConnectorConfig(props);
        Map<String, String> topicMappings = config.getTopicMappings();

        assertThat(topicMappings).containsKey("stocks");
        assertThat(topicMappings.get("stocks"))
                .isEqualTo("item-template.stock-template,item-template.stock-template-2");

        assertThat(topicMappings).containsKey("orders");
        assertThat(topicMappings.get("orders")).isEqualTo("item-template.order-template");
    }
}
