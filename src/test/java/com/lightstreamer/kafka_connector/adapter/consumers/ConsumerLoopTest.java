package com.lightstreamer.kafka_connector.adapter.consumers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import com.lightstreamer.interfaces.data.SubscriptionException;
import com.lightstreamer.kafka_connector.adapter.ConnectorConfigurator.ConsumerLoopConfig;
import com.lightstreamer.kafka_connector.adapter.config.TopicsConfig;
import com.lightstreamer.kafka_connector.adapter.config.TopicsConfig.TopicConfiguration;
import com.lightstreamer.kafka_connector.adapter.mapping.Fields.FieldMappings;
import com.lightstreamer.kafka_connector.adapter.mapping.Items;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.Item;
import com.lightstreamer.kafka_connector.adapter.mapping.Items.ItemTemplates;
import com.lightstreamer.kafka_connector.adapter.test_utils.SelectorsSuppliers;

class TestConsumerLoopTest extends AbstractConsumerLoop<String, String> {

    TestConsumerLoopTest(ConsumerLoopConfig<String, String> config) {
        super(config);
    }

}

class TestLoopConfig implements ConsumerLoopConfig<String, String> {

    private final TopicsConfig topicsConfig;

    TestLoopConfig(TopicsConfig topicsConfig) {
        this.topicsConfig = topicsConfig;
    }

    @Override
    public Properties consumerProperties() {
        return new Properties();
    }

    @Override
    public FieldMappings<String, String> fieldMappings() {
        throw new UnsupportedOperationException("Unimplemented method 'fieldMappings'");
    }

    @Override
    public ItemTemplates<String, String> itemTemplates() {
        return Items.templatesFrom(topicsConfig, SelectorsSuppliers.string());
    }

    @Override
    public Deserializer<String> keyDeserializer() {
        throw new UnsupportedOperationException("Unimplemented method 'keyDeserializer'");
    }

    @Override
    public Deserializer<String> valueDeserializer() {
        throw new UnsupportedOperationException("Unimplemented method 'valueDeserializer'");
    }

}

public class ConsumerLoopTest {

    private static TestConsumerLoopTest consumerLoopTest() {
        TopicConfiguration t = new TopicConfiguration("aTopic", "anItemTemplate");
        TopicsConfig topicsConfig = TopicsConfig.of(t);
        ConsumerLoopConfig<String, String> c = new TestLoopConfig(topicsConfig);

        TestConsumerLoopTest consumerLoopTest = new TestConsumerLoopTest(c);
        return consumerLoopTest;
    }

    @Test
    public void shouldSubscribe() throws SubscriptionException {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        assertThat(consumerLoopTest.getItemsCounter()).isEqualTo(0);
        Object itemHandle = new Object();
        Item item = consumerLoopTest.subscribe("anItemTemplate", itemHandle);
        assertThat(item).isEqualTo(Items.itemFrom("anItemTemplate", itemHandle));
        assertThat(consumerLoopTest.getItemsCounter()).isEqualTo(1);
    }

    @Test
    public void shouldNotSubscribe() {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        Object itemHandle = new Object();
        assertThrows(SubscriptionException.class, () -> consumerLoopTest.subscribe("unregisteredTemplate", itemHandle));
    }

    @Test
    public void shouldUnsubscribe() throws SubscriptionException {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        Object itemHandle = new Object();
        Item item = consumerLoopTest.subscribe("anItemTemplate", itemHandle);
        Item removed = consumerLoopTest.unsubscribe("anItemTemplate");
        assertThat(removed).isSameInstanceAs(item);
        assertThat(consumerLoopTest.getItemsCounter()).isEqualTo(0);
    }

    @Test
    public void shouldNotUnsubscribe() {
        TestConsumerLoopTest consumerLoopTest = consumerLoopTest();
        assertThrows(SubscriptionException.class, () -> consumerLoopTest.unsubscribe("anItemTemplate"));
    }

}
