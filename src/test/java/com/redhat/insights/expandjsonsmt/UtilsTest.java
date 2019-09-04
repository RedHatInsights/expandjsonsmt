package com.redhat.insights.expandjsonsmt;

import org.bson.BsonDocument;
import org.junit.Test;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void replaceUnsupportedKeyCharacters1(){
        String out = Utils.replaceUnsupportedKeyCharacters(BsonDocument.parse("{\"some-field\":\"val\"}")).toString();
        assertEquals("{\"some_field\": \"val\"}", out);
    }

    @Test
    public void replaceUnsupportedKeyCharacters2(){
        String out = Utils.replaceUnsupportedKeyCharacters(BsonDocument.parse("{\"some-long-field\":\"some-long-val\"}")).toString();
        assertEquals("{\"some_long_field\": \"some-long-val\"}", out);
    }

    @Test
    public void replaceUnsupportedKeyCharacters3(){
        String out = Utils.replaceUnsupportedKeyCharacters(BsonDocument
                .parse("{\"some-field\": {\"some-other-val\": \"some-val\"}}")).toString();
        assertEquals("{\"some_field\": {\"some_other_val\": \"some-val\"}}", out);
    }

    @Test
    public void replaceUnsupportedKeyCharacters4(){
        String out = Utils.replaceUnsupportedKeyCharacters(BsonDocument
                .parse("{\"some-field\": [{\"key-1\": 1}, {\"key-2\": \"val-1\"}]}")).toString();
        assertEquals("{\"some_field\": [{\"key_1\": 1}, {\"key_2\": \"val-1\"}]}", out);
    }
}
