// processors/LeqAlertProcessorSimple.java
package com.example.processors;

import com.example.config.KafkaConfig;
import com.example.models.Alert;
import com.example.models.LeqData;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

public class LeqAlertProcessorSimple {
    private static final Gson gson = new Gson();
    private static final double THRESHOLD = 45.0;

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(KafkaConfig.LEQ_TOPIC);

        sourceStream
            .filter((key, value) -> {
                try {
                    gson.fromJson(value, LeqData.class);
                    return true;
                } catch (Exception e) {
                    return false;
                }
            })
            .mapValues(value -> gson.fromJson(value, LeqData.class))
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
            .aggregate(
                () -> new LeqAggregator(),
                (key, data, agg) -> {
                    agg.addLeq(data.getLeq());
                    agg.updateMetadata(data);
                    return agg;
                },
                Materialized.<String, LeqAggregator, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("leq-window-simple")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.serdeFrom(
                        (topic, data) -> gson.toJson(data).getBytes(),
                        (topic, bytes) -> gson.fromJson(new String(bytes), LeqAggregator.class)
                    ))
            )
            .toStream()
            .filter((key, agg) -> agg.getCount() > 0 && agg.getAverage() > THRESHOLD)
            // CORREÇÃO 1: Usar mapValues + selectKey em vez de map
            .mapValues((windowedKey, agg) -> {
                double avg = agg.getAverage();
                long windowEnd = windowedKey.window().end();
                String sessionId = windowedKey.key();
                
                Alert alert = new Alert(
                    Alert.AlertType.CRITICAL_INTELLIGIBILITY,
                    sessionId,
                    String.valueOf(windowEnd),
                    agg.getLocation(),
                    agg.getClassroom(),
                    Alert.Severity.CRITICAL,
                    String.format("Inteligibilidade crítica: média de %.2f dB nos últimos 10s (limite: %.2f dB)", 
                                 avg, THRESHOLD)
                );
                
                return gson.toJson(alert);
            })
            // CORREÇÃO 2: Extrair a chave original para String
            .selectKey((windowedKey, value) -> windowedKey.key())
            // CORREÇÃO 3: O .to() agora funciona porque a chave é String
            .to(KafkaConfig.ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
    
    // Classe Agregadora
    static class LeqAggregator {
        private double sum = 0;
        private int count = 0;
        private String location = "unknown";
        private String classroom = "unknown";
        private String lastTimestamp = "";
        
        public void addLeq(double value) {
            sum += value;
            count++;
        }
        
        public void updateMetadata(LeqData data) {
            if (data.getLocation() != null && !data.getLocation().isEmpty()) {
                this.location = data.getLocation();
            }
            if (data.getClassroom() != null && !data.getClassroom().isEmpty()) {
                this.classroom = data.getClassroom();
            }
            if (data.getTimestamp() != null) {
                this.lastTimestamp = data.getTimestamp();
            }
        }
        
        public double getAverage() {
            return count > 0 ? sum / count : 0;
        }
        
        public int getCount() { return count; }
        public String getLocation() { return location; }
        public String getClassroom() { return classroom; }
        public String getLastTimestamp() { return lastTimestamp; }
    }
}