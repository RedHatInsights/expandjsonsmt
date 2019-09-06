package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.junit.Test;

import static org.junit.Assert.*;

public class DataConverterTest {

    @Test
    public void simple() {
        Schema schema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("code", Schema.INT32_SCHEMA)
                .build();
        BsonDocument bson = BsonDocument.parse("{\"city\":\"Studenec\",\"code\":123}");

        final Struct struct = (Struct)DataConverter.jsonStr2Struct(bson, schema);
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
        BsonDocument bson = BsonDocument.parse("{\"address\":{\"city\":\"Studenec\",\"code\":123},\"name\":\"Josef\"}");

        final Struct struct = (Struct)DataConverter.jsonStr2Struct(bson, schema);
        assertEquals(2, struct.schema().fields().size());
        assertEquals("Josef", struct.getString("name"));
        assertEquals("Studenec", struct.getStruct("address").getString("city"));
        assertEquals(new Integer(123), struct.getStruct("address").getInt32("code"));
    }

    @Test
    public void arrayOfStrings() {
        Schema schema = SchemaBuilder.struct()
                .field("arr", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA))
                .name("schema").build();
        BsonDocument bson = BsonDocument.parse("{\"arr\":[\"a\",\"b\"]}");

        final Struct struct = (Struct)DataConverter.jsonStr2Struct(bson, schema);
        assertEquals(1, struct.schema().fields().size());
        assertEquals(2, struct.getArray("arr").size());
        assertEquals("a", struct.getArray("arr").get(0));
        assertEquals("b", struct.getArray("arr").get(1));
    }

    @Test
    public void arrayOfInts() {
        Schema schema = SchemaBuilder.struct()
                .field("arr", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA))
                .name("schema").build();
        BsonDocument bson = BsonDocument.parse("{\"arr\":[11, 22]}");

        final Struct struct = (Struct)DataConverter.jsonStr2Struct(bson, schema);
        assertEquals(1, struct.schema().fields().size());
        assertEquals(2, struct.getArray("arr").size());
        assertEquals(11, struct.getArray("arr").get(0));
        assertEquals(22, struct.getArray("arr").get(1));
    }

    @Test
    public void arrayOfFloats() {
        Schema schema = SchemaBuilder.struct()
                .field("arr", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA))
                .name("schema").build();
        BsonDocument bson = BsonDocument.parse("{\"arr\":[1.1, 2.2]}");

        final Struct struct = (Struct)DataConverter.jsonStr2Struct(bson, schema);
        assertEquals(1, struct.schema().fields().size());
        assertEquals(2, struct.getArray("arr").size());
        assertEquals(1.1, struct.getArray("arr").get(0));
        assertEquals(2.2, struct.getArray("arr").get(1));
    }

    @Test
    public void arrayOfObjects() {
        Schema schema = SchemaBuilder.struct()
                .field("arr", SchemaBuilder.array(SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT32_SCHEMA)))
                .name("schema").build();
        BsonDocument bson = BsonDocument.parse("{\"arr\":[{\"a\":1},{\"a\":2}]}");

        final Struct struct = (Struct)DataConverter.jsonStr2Struct(bson, schema);
        assertEquals(1, struct.schema().fields().size());
        assertEquals(2, struct.getArray("arr").size());
        assertEquals(new Integer(1), ((Struct) struct.getArray("arr").get(0)).getInt32("a"));
        assertEquals(new Integer(2), ((Struct) struct.getArray("arr").get(1)).getInt32("a"));
    }
}