package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import static org.junit.Assert.*;

public class DataConverterTest {

    @Test
    public void simple() {
        Schema schema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("code", Schema.INT32_SCHEMA)
                .build();
        String jsonStr = "{\"city\":\"Studenec\",\"code\":123}";

        final Struct struct = DataConverter.jsonStr2Struct(jsonStr, schema);
        assertEquals(2, struct.schema().fields().size());
        assertEquals("Studenec", struct.getString("city"));
        assertEquals(new Integer(123), struct.getInt32("code"));
    }

    @Test
    public void complex() {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", SchemaBuilder.struct()
                        .field("city", Schema.STRING_SCHEMA)
                        .field("code", Schema.INT32_SCHEMA)
                        .name("schema.address").build())
                .name("schema").build();
        String jsonStr = "{\"address\":{\"city\":\"Studenec\",\"code\":123},\"name\":\"Josef\"}";

        final Struct struct = DataConverter.jsonStr2Struct(jsonStr, schema);
        assertEquals(2, struct.schema().fields().size());
        assertEquals("Josef", struct.getString("name"));
        assertEquals("Studenec", struct.getStruct("address").getString("city"));
        assertEquals(new Integer(123), struct.getStruct("address").getInt32("code"));
    }

}