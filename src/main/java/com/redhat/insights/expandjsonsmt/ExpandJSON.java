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
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        String OUTPUT_FIELDS = "outputFields";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELDS, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will be expanded to json object.")
            .define(ConfigName.OUTPUT_FIELDS, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM,
                            "Optional param - field to put expanded json object into.");

    private static final String PURPOSE = "json field expansion";

    private List<String> sourceFields;
    private List<String> outputFields;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceFields = config.getList(ConfigName.SOURCE_FIELDS);
        outputFields = config.getList(ConfigName.OUTPUT_FIELDS);
        if (outputFields.isEmpty()) {
            outputFields = sourceFields;
        }
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

        try {
            // update schema using JSON template from config
            final Schema updatedSchema = makeUpdatedSchema(value);

            // copy all fields and extract configured text field to JSON object
            final Struct updatedValue = new Struct(updatedSchema);
            for (Field field : value.schema().fields()) {
                if (sourceFields.contains(field.name())) {
                    String outputField = outputFields.get(sourceFields.indexOf(field.name()));
                    final String strVal = value.getString(field.name());
                    final Object fieldValue = DataConverter.jsonStr2Struct(strVal, updatedSchema.field(outputField).schema());
                    updatedValue.put(outputField, fieldValue);
                    if (outputField.equals(field.name())) {
                        // do not copy original value, if the input field equals output one
                        continue;
                    }
                }

                final Object fieldValue = value.get(field.name());
                updatedValue.put(field.name(), fieldValue);
            }
            return newRecord(record, updatedSchema, updatedValue);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            return record;
        }
    }

    private Schema makeUpdatedSchema(Struct value) {
        final Schema schema = value.schema();
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            final Schema fieldSchema;
            if (sourceFields.contains(field.name())) {
                String outputField = outputFields.get(sourceFields.indexOf(field.name()));
                String jsonStr = value.getString(field.name());
                SchemaParser.addJsonValueSchema(outputField, jsonStr, builder);
                if (outputField.equals(field.name())) {
                    // do not copy original schema, if the input field equals output one
                    continue;
                }
            }
            fieldSchema = field.schema();
            builder.field(field.name(), fieldSchema);
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
