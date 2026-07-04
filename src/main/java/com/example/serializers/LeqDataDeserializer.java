package com.example.serializers;

import com.example.models.LeqData;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

public class LeqDataDeserializer implements Deserializer<LeqData> {
    private final Gson gson = new Gson();
    
    @Override
    public LeqData deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return gson.fromJson(new String(data), LeqData.class);
    }
}