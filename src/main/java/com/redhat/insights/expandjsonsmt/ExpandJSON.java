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
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

abstract class ExpandJSON<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Filter or rename fields."
            + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
            + "or value (<code>" + Value.class.getName() + "</code>).";

    interface ConfigName {
        String SOURCE_FIELD_NAME = "sourceFieldName";
        String SOURCE_FIELD_SCHEMA = "sourceFieldSchema";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELD_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will be expanded to json object.")
            .define(ConfigName.SOURCE_FIELD_SCHEMA, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Source field json schema.");
    // TODO validate config params

    private static final String PURPOSE = "json field expansion";

    private String sourceFieldName;
    private String sourceFieldSchema;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceFieldName = config.getString(ConfigName.SOURCE_FIELD_NAME);
        sourceFieldSchema = config.getString(ConfigName.SOURCE_FIELD_SCHEMA);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            final Object fieldValue = e.getValue();
            updatedValue.put(fieldName, fieldValue);
        }

        final Map<String, Object> obj = new HashMap<String, Object>() {{
            put("a", 1);
        }};

        updatedValue.put("obj", obj);

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            Object fieldValue;
            if (field.name().equals(sourceFieldName)) {
                String strVal = value.getString(field.name());
                JSONObject jsonObj = new JSONObject(strVal);
                fieldValue = jsonObj2Struct(jsonObj, updatedSchema.field(sourceFieldName).schema());
            } else {
                fieldValue = value.get(field.name());
            }
            updatedValue.put(field.name(), fieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Struct jsonObj2Struct(JSONObject jsonObj, Schema structSchema) {
        final Struct obj = new Struct(structSchema);
        for (Field field : structSchema.fields()) {
            Object val;
            if (!jsonObj.has(field.name())) {
                continue;
            }

            if (field.schema().equals(Schema.OPTIONAL_INT32_SCHEMA)) {
                val = jsonObj.getInt(field.name());
            } else if (field.schema().equals(Schema.OPTIONAL_STRING_SCHEMA)) {
                val = jsonObj.getString(field.name());
            } else {
                val = jsonObj2Struct(jsonObj.getJSONObject(field.name()), field.schema());
            }
            obj.put(field.name(), val);
        }
        return obj;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            Schema fieldSchema;
            if (field.name().equals(sourceFieldName)) {
                fieldSchema = SchemaLoader.loadSchema(sourceFieldSchema);
            } else {
                fieldSchema = field.schema();
            }
            builder.field(field.name(), fieldSchema);
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

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