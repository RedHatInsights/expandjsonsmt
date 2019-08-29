package com.redhat.insights.expandjsonsmt;

import org.junit.Test;

import org.apache.kafka.connect.data.Schema;

import static org.junit.Assert.*;

public class SchemaLoaderTest {

    @Test
    public void simple() {
        String avroJson = "{\"fields\":[{\"name\":\"account\",\"type\":\"string\"},"
                                     + "{\"name\":\"age\",\"type\":\"int\"}]}";
        Schema schema = SchemaLoader.loadSchema(avroJson);
        assertEquals(2, schema.fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("account").schema());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("age").schema());
    }

    @Test
    public void complex() {
        String avroJson = "{\"fields\":[{\"name\":\"account\",\"type\":\"string\"},"
                                     + "{\"name\":\"age\",\"type\":\"int\"},"
                                     + "{\"name\":\"address\",\"type\":\"record\",\"fields\":["
                                       + "{\"name\":\"street\",\"type\":\"string\"},"
                                       + "{\"name\":\"code\",\"type\":\"int\"}]}]}";
        Schema schema = SchemaLoader.loadSchema(avroJson);
        assertEquals(3, schema.fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("account").schema());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("age").schema());
        assertEquals(2, schema.field("address").schema().fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("address").schema().field("street").schema());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("address").schema().field("code").schema());
    }
}