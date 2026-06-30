package com.example.serializers;
import com.example.models.MultAVGs;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

public class MultAVGsSerializer implements org.apache.kafka.common.serialization.Serializer<MultAVGs> {
    private final Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, MultAVGs data) {
        if (data == null) return null;
        return gson.toJson(data).getBytes();
    }
}