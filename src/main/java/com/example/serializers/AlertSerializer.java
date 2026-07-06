package com.example.serializers;

import com.example.models.Alert;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

public class AlertSerializer implements Serializer<Alert> {
    private final Gson gson = new Gson();
    
    @Override
    public byte[] serialize(String topic, Alert data) {
        if (data == null) return null;
        return gson.toJson(data).getBytes();
    }
}