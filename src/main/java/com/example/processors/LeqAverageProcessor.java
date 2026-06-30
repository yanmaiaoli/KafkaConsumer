package com.example.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import com.example.models.MultAVGs;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import com.example.config.KafkaConfig;
import com.example.serializers.MultAVGsDeserializer;
import com.example.serializers.MultAVGsSerializer;

public class LeqAverageProcessor {
    private static final Gson gson = new Gson();
    
    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> sourceStream = builder.stream(KafkaConfig.AUDIO_TOPIC);
        
        sourceStream
            .filter((key, value) -> {
                try {
                    JsonObject json = gson.fromJson(value, JsonObject.class);
                    return json.has("audioEvent");
                } catch (Exception e) {
                    return false;
                }
            })
            .groupByKey()
            .aggregate(
                () -> new MultAVGs(),
                (key, value, avgs) -> {
                    try {
                        JsonObject json = gson.fromJson(value, JsonObject.class);
                        JsonObject audioEvent = json.getAsJsonObject("audioEvent");
                        
                        double leq = audioEvent.get("lpi").getAsDouble();
                        String time = audioEvent.get("timestamp").getAsString();
                        
                        avgs.addValue(leq);
                        
                        // Processar L10
                        try {
                            if (audioEvent.has("l10") && audioEvent.get("l10").isJsonPrimitive()) {
                                double l10 = audioEvent.get("l10").getAsDouble();
                                if (l10 > 0 && !Double.isNaN(l10) && !Double.isInfinite(l10)) {
                                    avgs.addValuel10(l10);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing L10: " + e.getMessage());
                        }
                        
                        // Processar L90
                        try {
                            if (audioEvent.has("l90") && audioEvent.get("l90").isJsonPrimitive()) {
                                double l90 = audioEvent.get("l90").getAsDouble();
                                if (l90 > 0 && !Double.isNaN(l90) && !Double.isInfinite(l90)) {
                                    avgs.addValuel90(l90);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing L90: " + e.getMessage());
                        }
                        
                        // Log
                        if (avgs.hasL10Values() && avgs.hasL90Values()) {
                            System.out.printf("%s | SESSION %s | Leq avg: %.2f dB (based on %d samples) | L10: %.2f dB | L90: %.2f dB%n", 
                                time, 
                                key.substring(0, Math.min(8, key.length())),
                                avgs.getAverage(),
                                avgs.getCount(),
                                avgs.getAveragel10(),
                                avgs.getAveragel90()
                            );
                        } else {
                            System.out.printf("%s | SESSION %s | Leq avg: %.2f dB (based on %d samples)%n", 
                                time, 
                                key.substring(0, Math.min(8, key.length())),
                                avgs.getAverage(),
                                avgs.getCount()
                            );
                        }
                        
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                    return avgs;
                },
                Materialized.<String, MultAVGs, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("leq-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.serdeFrom(new MultAVGsSerializer(), new MultAVGsDeserializer()))
            );
        
        return builder.build();
    }
}