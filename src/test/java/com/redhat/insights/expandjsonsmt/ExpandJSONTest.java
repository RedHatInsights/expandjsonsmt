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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExpandJSONTest {
    private ExpandJSON<SinkRecord> xform = new ExpandJSON.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("blacklist", "dont");
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Josef");
        value.put("age", 42);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(3, updatedValue.size());
        assertEquals("Josef", updatedValue.get("name"));
        assertEquals(42, updatedValue.get("age"));
        assertEquals(1, ((Map)updatedValue.get("obj")).get("a"));
    }

    @Test
    public void withSchema() {
        final Map<String, String> props = new HashMap<>();
        String sourceSchemaJSON = "{\"fields\":[{\"name\":\"city\",\"type\":\"string\"},"
                                     + "{\"name\":\"code\",\"type\":\"int\"}]}";
        props.put("sourceFieldSchema", sourceSchemaJSON);
        props.put("sourceFieldName", "address");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("address", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("name", "Josef");
        value.put("age", 42);
        value.put("address","{\"city\":\"Studenec\",\"code\":123}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("age"));
        assertEquals("Josef", updatedValue.getString("name"));
        assertEquals("Studenec", updatedValue.getStruct("address").getString("city"));
        assertEquals(new Integer(123), updatedValue.getStruct("address").getInt32("code"));
    }

    @Test
    public void withSchemaMissingField() {
        /**
         * Ensure missing values will be replaced as empty
         */

        final Map<String, String> props = new HashMap<>();
        String sourceSchemaJSON = "{\"fields\":[{\"name\":\"city\",\"type\":\"string\"},"
                                     + "{\"name\":\"code\",\"type\":\"int\"}]}";
        props.put("sourceFieldSchema", sourceSchemaJSON);
        props.put("sourceFieldName", "address");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("address", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("name", "Josef");
        value.put("age", 42);
        value.put("address","{}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("age"));
        assertEquals("Josef", updatedValue.getString("name"));
        assertNull(updatedValue.getStruct("address").getString("city"));
        assertNull(updatedValue.getStruct("address").getInt32("code"));
    }
}
