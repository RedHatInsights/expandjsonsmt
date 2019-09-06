package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.ArrayList;

/**
 * Methods for conversion data to Kafka Connect structures.
 */
class DataConverter {

    /**
     * Convert JSON to Struct value according to inserted schema.
     * @param bson Parsed bson object.
     * @param schema Schema used for conversion.
     * @return Result object value.
     */
    static Struct jsonStr2Struct(BsonDocument bson, Schema schema) {
        final Struct struct;
        if (bson == null) {
            return null;
        } else {
            struct = bsonDocument2Struct(bson.asDocument(), schema);
        }
        return struct;
    }

    private static void convertFieldValue(String field, BsonValue bsonValue, Struct struct, Schema schema) {
        final Object obj = bsonValue2Object(bsonValue, schema);
        struct.put(field, obj);
    }

    private static Object bsonValue2Object(BsonValue value, Schema schema) {
        switch (value.getBsonType()) {
        case STRING:
            return value.asString().getValue();

        case OBJECT_ID:
            return value.asObjectId().getValue().toString();

        case DOUBLE:
            return value.asDouble().getValue();

        case BINARY:
            return value.asBinary().getData();

        case INT32:
            return value.asInt32().getValue();

        case INT64:
            return value.asInt64().getValue();

        case BOOLEAN:
            return value.asBoolean().getValue();

        case DATE_TIME:
            return value.asDateTime().getValue();

        case JAVASCRIPT:
            return value.asJavaScript().getCode();

        case TIMESTAMP:
            return value.asTimestamp().getTime();

        case DECIMAL128:
            return value.asDecimal128().getValue().toString();

        case DOCUMENT:
            return bsonDocument2Struct(value.asDocument(), schema);

        case ARRAY:
            return bsonArray2ArrayList(value.asArray(), schema);

        default:
            return null;
        }
    }

    private static Struct bsonDocument2Struct(BsonDocument doc, Schema schema) {
        final Struct struct = new Struct(schema);
        for(Field field : schema.fields()) {
            if (doc.containsKey(field.name())) {
                convertFieldValue(field.name(), doc.get(field.name()), struct, field.schema());
            }
        }
        return struct;
    }

    private static ArrayList<Object> bsonArray2ArrayList(BsonArray bsonArr, Schema schema) {
        final ArrayList<Object> arr = new ArrayList<>(bsonArr.size());
        for(BsonValue bsonValue : bsonArr.getValues()) {
            arr.add(bsonValue2Object(bsonValue, schema.valueSchema()));
        }
        return arr;
    }
}
