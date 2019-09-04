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

    @Test
    public void withArrayOfStrings() {
        String jsonStr = "{\"arr\":[\"\"]}";
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("json", jsonStr, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("json").schema().field("arr").schema()
                .valueSchema());
    }

    @Test
    public void withArrayOfIntegers() {
        String jsonStr = "{\"arr\":[0]}";
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("json", jsonStr, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("json").schema().field("arr").schema()
                .valueSchema());
    }

    @Test
    public void withArrayOfObjects() {
        String jsonStr = "{\"arr\":[{\"a\":0}]}";
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("json", jsonStr, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("json").schema().field("arr").schema()
                .valueSchema().field("a").schema());
    }
}