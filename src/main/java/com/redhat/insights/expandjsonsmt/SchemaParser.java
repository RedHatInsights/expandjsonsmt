package com.redhat.insights.expandjsonsmt;

import java.util.Map.Entry;

import org.apache.kafka.connect.errors.ConnectException;
import org.bson.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Kafka Connect parsing schema methods.
 */
class SchemaParser {

    /**
     * Add Struct schema according to JSON string.
     * @param field Output field to store JSON object.
     * @param bson Parsed document or null.
     * @param builder SchemaBuilder used to build schema.
     */
    static void addJsonValueSchema(String field, BsonDocument bson, SchemaBuilder builder) {
        final Schema fieldSchema;
        if (bson == null) {
            fieldSchema = SchemaBuilder.struct().name(builder.name() + "." + field).optional().build();
        } else {
            fieldSchema = bsonDocument2Schema(field, bson.asDocument(), builder);
        }
        builder.field(field, fieldSchema);
    }

    private static void addFieldSchema(Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {
        final String key = keyValuesforSchema.getKey();
        final BsonValue bsonValue = keyValuesforSchema.getValue();
        final Schema schema = bsonValue2Schema(key, bsonValue, builder);
        if (schema != null) {
            builder.field(key, schema);
        }
    }

    private static Schema bsonValue2Schema(String key, BsonValue bsonValue, SchemaBuilder builder) {
        switch (bsonValue.getBsonType()) {
        case NULL:
        case STRING:
        case JAVASCRIPT:
        case OBJECT_ID:
        case DECIMAL128:
            return Schema.OPTIONAL_STRING_SCHEMA;

        case DOUBLE:
            return Schema.OPTIONAL_FLOAT64_SCHEMA;

        case BINARY:
            return Schema.OPTIONAL_BYTES_SCHEMA;

        case INT32:
        case TIMESTAMP:
            return Schema.OPTIONAL_INT32_SCHEMA;

        case INT64:
        case DATE_TIME:
            return Schema.OPTIONAL_INT64_SCHEMA;

        case BOOLEAN:
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;

        case DOCUMENT:
            return bsonDocument2Schema(key, bsonValue.asDocument(), builder);

        case ARRAY:
            return bsonArray2Schema(key, bsonValue.asArray(), builder);

        default:
            return null;
        }
    }

    private static Schema bsonDocument2Schema(String key, BsonDocument doc, SchemaBuilder builder) {
        final SchemaBuilder fieldSchemaBuilder = SchemaBuilder.struct().name(builder.name() + "." + key).optional();
        for(Entry<String, BsonValue> entry : doc.entrySet()) {
            addFieldSchema(entry, fieldSchemaBuilder);
        }
        final Schema fieldSchema = fieldSchemaBuilder.build();
        return fieldSchema;
    }

    private static Schema bsonArray2Schema(String key, BsonArray bsonArr, SchemaBuilder builder) {
        final Schema memberSchema;
        if (bsonArr.isEmpty()){
            memberSchema = Schema.OPTIONAL_STRING_SCHEMA;
        } else {
            BsonType valueType = bsonArr.get(0).getBsonType();
            for (BsonValue element: bsonArr.asArray()) {
                if (element.getBsonType() != valueType) {
                    throw new ConnectException("Field " + key + " of schema " + builder.name() + " is not a homogenous array.");
                }
            }

            memberSchema = bsonValue2Schema(key + "." + "member", bsonArr.get(0), builder);
            if (memberSchema == null) {
                throw new ConnectException(String.format("Array '%s' has unrecognized member schema.", key));
            }
        }

        Schema arrSchema = SchemaBuilder.array(memberSchema).name(builder.name() + "." + key).optional().build();
        return arrSchema;
    }
}
