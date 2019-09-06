package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.bson.BsonDocument;
import org.junit.Test;

import org.apache.kafka.connect.data.Schema;

import static org.junit.Assert.*;

public class SchemaParserTest {

    @Test
    public void simple() {
        BsonDocument bson = BsonDocument.parse("{\"name\":\"Josef\",\"age\":31}");
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("person", bson, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(2, schema.field("person").schema().fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("person").schema().field("name").schema());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("person").schema().field("age").schema());
    }

    @Test
    public void complex() {
        BsonDocument bson = BsonDocument.parse("{\"name\":{\"first\":\"Josef\",\"last\":\"Hak\"},\"age\":31}");
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("person", bson, builder);
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
        BsonDocument bson = BsonDocument.parse("{\"arr\":[\"\"]}");
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("json", bson, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, schema.field("json").schema().field("arr").schema()
                .valueSchema());
    }

    @Test
    public void withArrayOfIntegers() {
        BsonDocument bson = BsonDocument.parse("{\"arr\":[0]}");
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("json", bson, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("json").schema().field("arr").schema()
                .valueSchema());
    }

    @Test
    public void withArrayOfObjects() {
        BsonDocument bson = BsonDocument.parse("{\"arr\":[{\"a\":0}]}");
        SchemaBuilder builder = SchemaBuilder.struct();
        SchemaParser.addJsonValueSchema("json", bson, builder);
        Schema schema = builder.build();
        assertEquals(1, schema.fields().size());
        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, schema.field("json").schema().field("arr").schema()
                .valueSchema().field("a").schema());
    }
}