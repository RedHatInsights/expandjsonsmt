package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonValue;


class DataConverter {

    static Struct jsonStr2Struct(String jsonStr, Schema schema) {
        final BsonDocument rawDoc = BsonDocument.parse(jsonStr);
        final BsonDocument doc = Utils.replaceUnsupportedKeyCharacters(rawDoc);
        final Struct struct = bsonDocument2Struct(doc, schema);
        return struct;
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

    private static void convertFieldValue(String field, BsonValue value, Struct struct, Schema schema) {
        final Object colValue;

        switch (value.getBsonType()) {
        case STRING:
            colValue = value.asString().getValue();
            break;

        case OBJECT_ID:
            colValue = value.asObjectId().getValue().toString();
            break;

        case DOUBLE:
            colValue = value.asDouble().getValue();
            break;

        case BINARY:
            colValue = value.asBinary().getData();
            break;

        case INT32:
            colValue = value.asInt32().getValue();
            break;

        case INT64:
            colValue = value.asInt64().getValue();
            break;

        case BOOLEAN:
            colValue = value.asBoolean().getValue();
            break;

        case DATE_TIME:
            colValue = value.asDateTime().getValue();
            break;

        case JAVASCRIPT:
            colValue = value.asJavaScript().getCode();
            break;

        case TIMESTAMP:
            colValue = value.asTimestamp().getTime();
            break;

        case DECIMAL128:
            colValue = value.asDecimal128().getValue().toString();
            break;

        case DOCUMENT:
            colValue = bsonDocument2Struct(value.asDocument(), schema);
            break;
        default:
            colValue = null;
            break;
        }
        struct.put(field, colValue);
    }

}
