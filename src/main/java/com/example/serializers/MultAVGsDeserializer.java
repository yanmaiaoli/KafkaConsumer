package com.example.serializers;
import com.example.models.MultAVGs;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

public class MultAVGsDeserializer implements org.apache.kafka.common.serialization.Deserializer<MultAVGs> {
    private final Gson gson = new Gson();

    @Override
    public MultAVGs deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return gson.fromJson(new String(data), MultAVGs.class);
    }
}