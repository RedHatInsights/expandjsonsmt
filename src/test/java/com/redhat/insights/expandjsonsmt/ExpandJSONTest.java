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
        props.put("sourceField", "address");
        props.put("jsonTemplate", "{\"city\":\"\",\"code\":0}");

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
        props.put("sourceField", "address");
        props.put("jsonTemplate", "{\"city\":\"\",\"code\":0}");

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
        props.put("sourceField", "location");
        props.put("jsonTemplate", "{\"country\":\"\",\"address\":{\"city\":\"\",\"code\":0}}");

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
        props.put("sourceField", "address");
        props.put("jsonTemplate", "{\"city\":\"\",\"code\":0}");
        props.put("outputField", "addressObj");

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
    public void dashKeysReplacement() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "json_obj");
        props.put("jsonTemplate", "{\"text-1\":\"\",\"obj-1-1\":{\"text-2\":\"\",\"num-1\":0}}");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("json_obj", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("json_obj","{\"text-1\":\"a-b-c\",\"obj-1-1\":{\"text-2\":\"ab-cd\",\"num-1\":5}}");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals(2, updatedValue.getStruct("json_obj").schema().fields().size());
        assertEquals("a-b-c", updatedValue.getStruct("json_obj").getString("text_1"));
        assertEquals("ab-cd", updatedValue.getStruct("json_obj").getStruct("obj_1_1")
                .getString("text_2"));
        assertEquals(new Integer(5), updatedValue.getStruct("json_obj").getStruct("obj_1_1")
                .getInt32("num_1"));
    }

    @Test
    public void arrayCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "obj");
        props.put("jsonTemplate", "{\"ar1\":[0]}");

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
}
