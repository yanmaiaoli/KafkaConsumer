package com.example.serializers;

import com.example.models.Alert;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

public class AlertDeserializer implements Deserializer<Alert> {
    private final Gson gson = new Gson();
    
    @Override
    public Alert deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return gson.fromJson(new String(data), Alert.class);
    }
}