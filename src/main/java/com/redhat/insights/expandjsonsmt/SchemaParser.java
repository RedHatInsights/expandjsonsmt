package com.redhat.insights.expandjsonsmt;

import java.util.Map.Entry;

import org.bson.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

class SchemaParser {

    static void addJsonValueSchema(String field, String jsonString, SchemaBuilder builder) {
        final BsonDocument rawDoc = BsonDocument.parse(jsonString);
        final BsonDocument doc = Utils.replaceUnsupportedKeyCharacters(rawDoc);
        addBsonDocumentFieldSchema(field, doc, builder);
    }

    private static void addBsonDocumentFieldSchema(String field, BsonDocument doc, SchemaBuilder builder) {
        final SchemaBuilder fieldSchemaBuilder = SchemaBuilder.struct().name(builder.name() + "." + field).optional();
        for(Entry<String, BsonValue> entry : doc.entrySet()) {
            addFieldSchema(entry, fieldSchemaBuilder);
        }
        final Schema fieldSchema = fieldSchemaBuilder.build();
        builder.field(field, fieldSchema);
    }

    private static void addFieldSchema(Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {
        final String key = keyValuesforSchema.getKey();
        final BsonType type = keyValuesforSchema.getValue().getBsonType();

        switch (type) {

        case NULL:
        case STRING:
        case JAVASCRIPT:
        case OBJECT_ID:
        case DECIMAL128:
            builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            break;

        case DOUBLE:
            builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
            break;

        case BINARY:
            builder.field(key, Schema.OPTIONAL_BYTES_SCHEMA);
            break;

        case INT32:
        case TIMESTAMP:
            builder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
            break;

        case INT64:
        case DATE_TIME:
            builder.field(key, Schema.OPTIONAL_INT64_SCHEMA);
            break;

        case BOOLEAN:
            builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            break;

        case DOCUMENT:
            addBsonDocumentFieldSchema(key, keyValuesforSchema.getValue().asDocument(), builder);
            break;
        default:
            break;
        }
    }
}
