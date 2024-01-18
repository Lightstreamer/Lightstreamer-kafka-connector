package com.lightstreamer.kafka_connector.adapter;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.config.ConnectorConfig;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Schema;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.Selectors;
import com.lightstreamer.kafka_connector.adapter.mapping.selectors.json.JsonNodeDeserializer;

public class ConnectorConfiguratorTest {

    private Map<String, String> standardParameters() {
        Map<String, String> adapterParams = new HashMap<>();
        adapterParams.put(ConnectorConfig.BOOTSTRAP_SERVERS, "server:8080,server:8081");
        adapterParams.put(ConnectorConfig.GROUP_ID, "group-id");
        adapterParams.put(ConnectorConfig.KEY_EVALUATOR_TYPE, "RAW");
        adapterParams.put(ConnectorConfig.VALUE_EVALUATOR_TYPE, "JSON");
        adapterParams.put("item.item-template1", "item1");
        adapterParams.put("item.item-template2", "item2");
        adapterParams.put("map.topic1.to", "item-template1,item-template2");
        adapterParams.put("field.fieldName1", "VALUE.name");
        return adapterParams;
    }

    @Test
    public void shouldConfigure() throws IOException {
        Path adapterDir = Files.createTempDirectory("adapter_dir");
        ConnectorConfigurator configurator = new ConnectorConfigurator(adapterDir.toFile());
        ConsumerLoopConfig<?, ?> loopConfig = configurator.configure(standardParameters());

        Properties consumerProperties = loopConfig.consumerProperties();
        assertThat(consumerProperties).containsExactly(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "server:8080,server:8081",
                ConsumerConfig.GROUP_ID_CONFIG, "group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Selectors<?, ?> fieldsSelectors = loopConfig.fieldsSelectors();
        Schema schema = fieldsSelectors.schema();
        assertThat(schema.name()).isEqualTo("fields");
        assertThat(schema.keys()).containsExactly("fieldName1");

        ItemTemplates<?, ?> itemTemplates = loopConfig.itemTemplates();
        assertThat(itemTemplates.topics()).containsExactly("topic1");
        assertThat(itemTemplates.selectors().map(s -> s.schema().name())).containsExactly("item-template1", "item-template2");
        
        assertThat(loopConfig.keyDeserializer().getClass()).isEqualTo(StringDeserializer.class);
        assertThat(loopConfig.valueDeserializer().getClass()).isEqualTo(JsonNodeDeserializer.class);
        loopConfig.valueDeserializer();
    }
}
