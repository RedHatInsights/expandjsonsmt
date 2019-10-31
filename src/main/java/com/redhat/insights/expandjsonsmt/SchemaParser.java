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
     * Get Struct schema according to input document.
     * @param doc Parsed document or null.
     */
    static Schema bsonDocument2Schema(BsonDocument doc) {
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct().optional();
        if (doc != null) {
            for(Entry<String, BsonValue> entry : doc.entrySet()) {
                addFieldSchema(entry, schemaBuilder);
            }
        }
        final Schema fieldSchema = schemaBuilder.build();
        return fieldSchema;
    }


    private static void addFieldSchema(Entry<String, BsonValue> keyValuesforSchema, SchemaBuilder builder) {
        final String key = keyValuesforSchema.getKey();
        final BsonValue bsonValue = keyValuesforSchema.getValue();
        final Schema schema = bsonValue2Schema(bsonValue);
        if (schema != null) {
            builder.field(key, schema);
        }
    }

    private static Schema bsonValue2Schema(BsonValue bsonValue) {
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
            return bsonDocument2Schema(bsonValue.asDocument());

        case ARRAY:
            return bsonArray2Schema(bsonValue.asArray());

        default:
            return null;
        }
    }

    /**
     * Get first not-null member value of the array.
     * Check the same schema of all array members.
     */
    private static BsonValue getArrayElement(BsonArray bsonArray) {
        BsonValue bsonValue = new BsonNull();
        // Get first not-null element type
        for (BsonValue element : bsonArray.asArray()) {
           if (element.getBsonType() != BsonType.NULL) {
               bsonValue = element;
               break;
           }
        }

        // validate all members type
        for (BsonValue element: bsonArray.asArray()) {
            if (element.getBsonType() != bsonValue.getBsonType() && element.getBsonType() != BsonType.NULL) {
                throw new ConnectException(String.format("Field is not a homogenous array (%s x %s).",
                        bsonValue.toString(), element.getBsonType().toString()));
            }
        }
        return bsonValue;
    }

    private static Schema bsonArray2Schema(BsonArray bsonArr) {
        final Schema memberSchema;
        if (bsonArr.isEmpty()){
            memberSchema = Schema.OPTIONAL_STRING_SCHEMA;
        } else {
            final BsonValue elementSample = getArrayElement(bsonArr);
            memberSchema = bsonValue2Schema(elementSample);
            if (memberSchema == null) {
                throw new ConnectException("Array has unrecognized member schema.");
            }
        }

        Schema arrSchema = SchemaBuilder.array(memberSchema).optional().build();
        return arrSchema;
    }
}
