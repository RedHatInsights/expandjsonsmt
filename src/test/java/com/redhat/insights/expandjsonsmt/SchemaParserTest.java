package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import org.apache.kafka.connect.data.Schema;

import static org.junit.Assert.*;

public class SchemaParserTest {

    @Test
    public void simple() {
        String jsonStr = "{\"name\":\"Josef\",\"age\":31}";
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("person", jsonStr, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(2, schema.field("person").schema().fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("person").schema().field("name").schema());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("person").schema().field("age").schema());
    }

    @Test
    public void complex() {
        String jsonStr = "{\"name\":{\"first\":\"Josef\",\"last\":\"Hak\"},\"age\":31}";

        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("person", jsonStr, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(2, schema.field("person").schema().fields().size());
        assertEquals(2, schema.field("person").schema().field("name").schema().fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("person")
                .schema().field("name").schema().field("first").schema());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("person")
                .schema().field("name").schema().field("last").schema());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("person").schema().field("age").schema());
    }
}