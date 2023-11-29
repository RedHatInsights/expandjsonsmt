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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class ExpandArrayTest {

    @Parameters
    public static Iterable<String[]> data() {
        return Arrays.asList(new String[][] {
            {
                "{\"data\": [{\"name\": \"Alpha\"}, {\"name\": \"Bravo\", \"version\": 1}]}",
                "Struct{obj=Struct{data=[Struct{name=Alpha}, Struct{name=Bravo,version=1}]}}"
            },
            {
                "{\"data\": [{\"foo\": \"A\"}, {\"bar\": \"B\"}]}",
                "Struct{obj=Struct{data=[Struct{foo=A}, Struct{bar=B}]}}"
            },
            {
                "{\"data\": [{\"foo\": \"A\"}, {\"foo\": \"B\"}, {\"foo\": \"C\"}]}",
                "Struct{obj=Struct{data=[Struct{foo=A}, Struct{foo=B}, Struct{foo=C}]}}"
            },
            {
                "{\"data\": [2146192818, 2146574830, 2147639482]}",
                "Struct{obj={\"data\": [2146192818, 2146574830, 2147639482]}}"
            }
        });
    }

    private ExpandJSON<SinkRecord> xform = new ExpandJSON.Value<>();

    private final String input;
    private final String expected;

    public ExpandArrayTest (String input, String expected) {
        this.input = input;
        this.expected = expected;
    }

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void testArray() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceFields", "obj");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct().field("obj", Schema.STRING_SCHEMA).build();
        final Struct value = new Struct(schema);
        value.put("obj",this.input);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(this.expected, updatedValue.toString());
    }
}
