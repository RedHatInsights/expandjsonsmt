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

import static org.junit.Assert.*;

public class ExpandJSONTest {
    private ExpandJSON<SinkRecord> xform = new ExpandJSON.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(new HashMap<>());

        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Josef");
        value.put("age", 42);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);
        assertNull(transformedRecord);
    }

    @Test
    public void basicCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "address");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("address", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("address","{\"city\":\"Studenec\",\"code\":123}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals("Studenec", updatedValue.getStruct("address").getString("city"));
        assertEquals(new Integer(123), updatedValue.getStruct("address").getInt32("code"));
    }

    @Test
    public void missingField() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "address");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("address", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("address","{}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals("Struct{}", updatedValue.getStruct("address").toString());
    }

    @Test
    public void complex() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "location");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("location", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("name", "Josef");
        value.put("age", 42);
        value.put("location",
                "{\"country\":\"Czech Republic\",\"address\":{\"city\":\"Studenec\",\"code\":123},\"add\":0}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("age"));
        assertEquals("Josef", updatedValue.getString("name"));
        assertEquals("Studenec", updatedValue.getStruct("location").getStruct("address")
                .getString("city"));
        assertEquals(new Integer(123), updatedValue.getStruct("location").getStruct("address")
                .getInt32("code"));
        assertEquals("Czech Republic", updatedValue.getStruct("location").getString("country"));
    }

    @Test
    public void outputField() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "address");
        props.put("outputFields", "addressObj");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("name", "Josef");
        String jsonStr = "{\"city\":\"Studenec\",\"code\":123}";
        value.put("address", jsonStr);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals("Josef", updatedValue.getString("name"));
        assertEquals(jsonStr, updatedValue.getString("address"));
        assertEquals("Studenec", updatedValue.getStruct("addressObj").getString("city"));
        assertEquals(new Integer(123), updatedValue.getStruct("addressObj").getInt32("code"));
    }

    @Test
    public void arrayCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct().field("obj", Schema.STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("obj","{\"ar1\":[1,2,3,4]}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals(4, updatedValue.getStruct("obj").getArray("ar1").size());
    }

    @Test
    public void nullValue() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "location,metadata");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("age", Schema.INT32_SCHEMA)
                .field("location", Schema.OPTIONAL_STRING_SCHEMA)
                .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("age", 42);
        value.put("location", "{\"country\":\"USA\"}");
        value.put("metadata", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("age"));
        assertEquals("USA", updatedValue.getStruct("location").getString("country"));
        assertNull(updatedValue.get("metadata"));
    }

    @Test
    public void arrayInRoot() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "arr1,arr2,arr3");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("arr1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr3", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("arr1","[1,2,3,4]");
        value.put("arr2", "[]");
        value.put("arr3", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(4, updatedValue.getStruct("arr1").getArray("array").size());
        assertEquals(0, updatedValue.getStruct("arr2").getArray("array").size());
        assertNull(updatedValue.getStruct("arr3"));
    }

    @Test
    public void malformated() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj1,obj2,arr");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("obj1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("obj2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("arr", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("obj1","{\"a\":\"msg\"}");
        value.put("obj2", "{malf");
        value.put("arr", "[]");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals("msg", updatedValue.getStruct("obj1").getString("a"));
        assertEquals(0, updatedValue.getStruct("arr").getArray("array").size());
        assertEquals("{malf",updatedValue.getStruct("obj2").getString("value"));
        assertTrue(updatedValue.getStruct("obj2").getString("error").startsWith("JSON reader"));
    }
}
