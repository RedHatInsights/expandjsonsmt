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

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

abstract class ExpandJSON<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpandJSON.class);

    public static final String OVERVIEW_DOC = "Filter or rename fields."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

    interface ConfigName {
        String SOURCE_FIELD = "sourceField";
        String JSON_TEMPLATE = "jsonTemplate";
        String OUTPUT_FIELD = "outputField";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will be expanded to json object.")
            .define(ConfigName.JSON_TEMPLATE, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Source field json template.")
            .define(ConfigName.OUTPUT_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                            "Field to put expanded json object into.");

    private static final String PURPOSE = "json field expansion";

    private String sourceField;
    private String jsonTemplate;
    private String outputField;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceField = config.getString(ConfigName.SOURCE_FIELD);
        jsonTemplate = config.getString(ConfigName.JSON_TEMPLATE);
        outputField = config.getString(ConfigName.OUTPUT_FIELD);
        if (outputField.equals("")) {
            outputField = sourceField;
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

        final Schema updatedSchema = makeUpdatedSchema(value);

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (field.name().equals(sourceField)) {
                final String strVal = value.getString(field.name());
                final Object fieldValue = DataConverter.jsonStr2Struct(strVal, updatedSchema.field(outputField).schema());
                updatedValue.put(outputField, fieldValue);
                if (sourceField.equals(outputField)) {
                    continue;
                }
            }

            final Object fieldValue = value.get(field.name());
            updatedValue.put(field.name(), fieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Struct value) {
        final Schema schema = value.schema();
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            final Schema fieldSchema;
            if (field.name().equals(sourceField)) {
                SchemaParser.addJsonValueSchema(outputField, jsonTemplate, builder);
                if (sourceField.equals(outputField)) {
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

    public static class Key<R extends ConnectRecord<R>> extends ExpandJSON<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

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