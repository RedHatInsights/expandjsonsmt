/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.insights.expandjsonsmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Main project class implementing JSON string transformation.
 */
abstract class ExpandJSON<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpandJSON.class);

    interface ConfigName {
        String SOURCE_FIELDS = "sourceFields";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELDS, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will be expanded to json object.");

    private static final String PURPOSE = "json field expansion";

    private List<String> sourceFields;
    private String delimiterSplit = "\\.";
    private String delimiterJoin = ".";

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceFields = config.getList(ConfigName.SOURCE_FIELDS);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            LOGGER.info("Schemaless records not supported");
            return null;
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final HashMap<String, BsonDocument> jsonParsedFields = parseJsonFields(value, sourceFields, delimiterSplit);

        final Schema updatedSchema = makeUpdatedSchema(null, value, jsonParsedFields);
        final Struct updatedValue = makeUpdatedValue(null, value, updatedSchema, jsonParsedFields);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private static String getStringValue(List<String> path, Struct value) {
        if (path.isEmpty()) {
            return null;
        } else if (path.size() == 1) {
            return value.getString(path.get(0));
        } else {
            return getStringValue(path.subList(1, path.size()), value.getStruct(path.get(0)));
        }
    }

    /**
     * Parse JSON objects from given string fields.
     * @param value Input record to read original string fields.
     * @param sourceFields List of fields to parse JSON objects from.
     * @return Collection of parsed JSON objects with field names.
     */
    private static HashMap<String, BsonDocument> parseJsonFields(Struct value, List<String> sourceFields,
                                                                 String levelDelimiter) {
        final HashMap<String, BsonDocument> bsons = new HashMap<>(sourceFields.size());
        for(String field : sourceFields){
            BsonDocument val;
            String[] pathArr = field.split(levelDelimiter);
            List<String> path = Arrays.asList(pathArr);
            final String jsonString = getStringValue(path, value);
            if (jsonString == null) {
                val = null;
            } else {
                try {
                    if (jsonString.startsWith("{")) {
                        val = BsonDocument.parse(jsonString);
                    } else if (jsonString.startsWith("[")) {
                        final BsonArray bsonArray = BsonArray.parse(jsonString);
                        val = new BsonDocument();
                        val.put("array", bsonArray);
                    } else {
                        String msg = String.format("Unable to parse filed '%s' starting with '%s'", field, jsonString.charAt(0));
                        throw new Exception(msg);
                    }
                } catch (Exception ex) {
                    LOGGER.warn(ex.getMessage(), ex);
                    val = new BsonDocument();
                    val.put("value", new BsonString(jsonString));
                    val.put("error", new BsonString(ex.getMessage()));
                }
            }
            bsons.put(field, val);
        }
        return bsons;
    }

    /**
     * Copy original fields value or take parsed JSONS from collection.
     * @param value Input value to copy fields from.
     * @param updatedSchema Schema for new output record.
     * @param jsonParsedFields Parsed JSON objects.
     * @return Output record with parsed JSON values.
     */
    private Struct makeUpdatedValue(String parentKey, Struct value, Schema updatedSchema, HashMap<String, BsonDocument> jsonParsedFields) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object fieldValue;
            final String absoluteKey = joinKeys(parentKey, field.name());
            if (jsonParsedFields.containsKey(absoluteKey)) {
                final BsonDocument parsedValue = jsonParsedFields.get(absoluteKey);
                fieldValue = DataConverter.jsonStr2Struct(parsedValue,
                        updatedSchema.field(field.name()).schema());
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                fieldValue = makeUpdatedValue(absoluteKey, value.getStruct(field.name()),
                        updatedSchema.field(field.name()).schema(), jsonParsedFields);
            } else {
                fieldValue = value.get(field.name());
            }
            updatedValue.put(field.name(), fieldValue);
        }
        return updatedValue;
    }

    private String joinKeys(String parent, String child) {
        if (parent == null) {
            return child;
        }
        return parent + delimiterJoin + child;
    }

    /**
     * Update schema using JSON template from config.
     * @param value Input value to take basic schema from.
     * @param jsonParsedFields Values of parsed json string fields.
     * @return New schema for output record.
     */
    private Schema makeUpdatedSchema(String parentKey, Struct value, HashMap<String, BsonDocument> jsonParsedFields) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        for (Field field : value.schema().fields()) {
            final Schema fieldSchema;
            final String absoluteKey = joinKeys(parentKey, field.name());
            if (jsonParsedFields.containsKey(absoluteKey)) {
                // TODO - simplify to BsonDocument 2 Struct conversion...
                SchemaParser.addJsonValueSchema(field.name(), jsonParsedFields.get(absoluteKey), builder);
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                fieldSchema = makeUpdatedSchema(absoluteKey, value.getStruct(field.name()), jsonParsedFields);
                builder.field(field.name(), fieldSchema); // todo: one place
            } else {
                fieldSchema = field.schema();
                builder.field(field.name(), fieldSchema);
            }
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() { }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends ExpandJSON<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
