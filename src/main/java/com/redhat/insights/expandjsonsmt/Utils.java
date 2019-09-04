package com.redhat.insights.expandjsonsmt;

import org.bson.*;

import java.util.Map;

class Utils {

    static BsonDocument replaceUnsupportedKeyCharacters(BsonDocument doc) {
        BsonDocument outDoc = new BsonDocument();
        for(Map.Entry<String, BsonValue> entry : doc.entrySet()) {
            BsonValue updatedValue;
            switch (entry.getValue().getBsonType()) {
                case DOCUMENT:
                    updatedValue = replaceUnsupportedKeyCharacters(entry.getValue().asDocument());
                    break;
                case ARRAY:
                    BsonArray arr2 = new BsonArray();
                    for (BsonValue arrVal : entry.getValue().asArray().getValues()) {
                        if (arrVal.getBsonType() == BsonType.DOCUMENT) {
                            arr2.add(replaceUnsupportedKeyCharacters(arrVal.asDocument()));
                        } else {
                            arr2.add(arrVal);
                        }
                    }
                    updatedValue = arr2;
                    break;
                default:
                    updatedValue = entry.getValue();
            }
            outDoc.put(entry.getKey().replace("-", "_"), updatedValue);
        }
        return outDoc;
    }
}
