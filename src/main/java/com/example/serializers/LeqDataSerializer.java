package com.example.serializers;

import com.example.models.LeqData;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

public class LeqDataSerializer implements Serializer<LeqData> {
    private final Gson gson = new Gson();
    
    @Override
    public byte[] serialize(String topic, LeqData data) {
        if (data == null) return null;
        return gson.toJson(data).getBytes();
    }
}