package com.example.serializers;

import com.example.models.HighAndLowsData;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

public class HighAndLowsDataSerializer implements Serializer<HighAndLowsData> {
    private final Gson gson = new Gson();
    
    @Override
    public byte[] serialize(String topic, HighAndLowsData data) {
        if (data == null) return null;
        return gson.toJson(data).getBytes();
    }
}