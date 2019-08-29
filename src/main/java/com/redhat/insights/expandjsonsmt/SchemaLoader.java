package com.redhat.insights.expandjsonsmt;

import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

class SchemaLoader {

    private static Schema jsonArray2Schema(JSONObject jsonobj) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        JSONArray jsonFields = jsonobj.getJSONArray("fields");
        for (int i = 0; i < jsonFields.length(); i++) {
            JSONObject jsonField = jsonFields.getJSONObject(i);
            Schema fieldSchema;
            String fieldType = jsonField.getString("type");
            switch (fieldType) {
                case "int":
                    fieldSchema = Schema.OPTIONAL_INT32_SCHEMA;
                    break;
                case "string":
                    fieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
                    break;
                case "record":
                    fieldSchema = jsonArray2Schema(jsonField);
                    break;
                default:
                    throw new Error(String.format("Unknown field type '%s'", fieldType));
            }
            schemaBuilder.field(jsonField.getString("name"), fieldSchema);
        }
        return schemaBuilder.optional().build();
    }

    static Schema loadSchema(String avroJson) {
        final JSONObject jsonobj = new JSONObject(avroJson);
        Schema schema = jsonArray2Schema(jsonobj);
        return schema;
    }
}
