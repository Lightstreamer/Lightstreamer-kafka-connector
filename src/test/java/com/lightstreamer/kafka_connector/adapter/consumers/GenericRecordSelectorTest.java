package com.lightstreamer.kafka_connector.adapter.consumers;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.lightstreamer.kafka_connector.adapter.consumers.avro.GenericRecordSelector;

public class GenericRecordSelectorTest {

    private static Schema valueSchema;
    private static Schema childrenSchema;

    @BeforeAll
    public static void before() throws IOException {
        ClassLoader classLoader = GenericRecordSelectorTest.class.getClassLoader();
        Schema.Parser parser = new Schema.Parser();

        valueSchema = parser.parse(classLoader.getResourceAsStream("value.avsc"));
        childrenSchema = parser.parse(classLoader.getResourceAsStream("array.avsc"));
    }

    @Test
    public void shouldExtractSimpleExpression() {
        GenericRecord parent = new GenericData.Record(valueSchema);
        parent.put("name", "joe");

        GenericRecordSelector selector = new GenericRecordSelector("name", "VALUE.name");
        assertThat(selector.extract(parent).text()).isEqualTo("joe");
    }

    @Test
    public void shouldExtractNestedExpression() {
        GenericRecord v1 = new GenericData.Record(valueSchema);
        v1.put("name", "joe");

        GenericRecord v1a = new GenericData.Record(valueSchema);
        v1a.put("name", "alex");

        GenericRecord v1b = new GenericData.Record(valueSchema);
        v1b.put("name", "anna");

        GenericRecord v1c = new GenericData.Record(valueSchema);
        v1c.put("name", "serena");

        GenericRecord v1ba = new GenericData.Record(valueSchema);
        v1ba.put("name", "gloria");

        GenericRecord v1bb = new GenericData.Record(valueSchema);
        v1bb.put("name", "terence");
        v1b.put("children", new GenericData.Array<>(childrenSchema, List.of(v1ba, v1bb)));

        v1.put("children", new GenericData.Array<>(childrenSchema, List.of(v1a, v1b, v1c)));

        GenericRecordSelector selector = new GenericRecordSelector("name", "VALUE.children[0].name");
        assertThat(selector.extract(v1).text()).isEqualTo("alex");

        selector = new GenericRecordSelector("name", "VALUE.children[1].name");
        assertThat(selector.extract(v1).text()).isEqualTo("anna");

        selector = new GenericRecordSelector("name", "VALUE.children[2].name");
        assertThat(selector.extract(v1).text()).isEqualTo("serena");

        selector = new GenericRecordSelector("name", "VALUE.children[1].children[0].name");
        assertThat(selector.extract(v1).text()).isEqualTo("gloria");

        selector = new GenericRecordSelector("name", "VALUE.children[1].children[1].name");
        assertThat(selector.extract(v1).text()).isEqualTo("terence");
    }
}
