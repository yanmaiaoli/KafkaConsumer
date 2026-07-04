package com.example.serializers;

import com.example.models.HighAndLowsData;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

public class HighAndLowsDataDeserializer implements Deserializer<HighAndLowsData> {
    private final Gson gson = new Gson();
    
    @Override
    public HighAndLowsData deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return gson.fromJson(new String(data), HighAndLowsData.class);
    }
}