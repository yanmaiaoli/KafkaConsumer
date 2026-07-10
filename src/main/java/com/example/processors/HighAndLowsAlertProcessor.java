package com.example.processors;

import com.example.config.KafkaConfig;
import com.example.models.Alert;
import com.example.models.HighAndLowsData;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class HighAndLowsAlertProcessor {
    private static final Gson gson = new Gson();
    private static final double L10_THRESHOLD = 55.0;
    private static final double L90_THRESHOLD = 20.0;
    private static final double INTRUSION_THRESHOLD = 20.0;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(
            KafkaConfig.HIGH_AND_LOWS_TOPIC,
            Consumed.with(Serdes.String(), Serdes.String())
        );

        // Abordagem 1: Apenas filter + flatMapValues - sem groupByKey, sem window
        sourceStream
            .filter((key, value) -> {
                try {
                    gson.fromJson(value, HighAndLowsData.class);
                    return true;
                } catch (Exception e) {
                    System.err.println("❌ Error deserializing HighAndLowsData: " + e.getMessage());
                    return false;
                }
            })
            .flatMapValues(value -> {
                HighAndLowsData data = gson.fromJson(value, HighAndLowsData.class);
                List<String> alerts = new ArrayList<>();
                
                double l10 = data.getL10();
                double l90 = data.getL90();
                double intrusion = data.getIntrusion();
                String sessionId = data.getSessionId();
                String timestamp = data.getTimestamp();
                String location = data.getLocation();
                String classroom = data.getClassroom();
                
                String timeFormatted = formatTimestamp(timestamp);
                String sessionShort = sessionId.substring(0, Math.min(8, sessionId.length()));
                
                // HIGH_PEAK
                if (l10 > L10_THRESHOLD) {
                    System.out.printf("🔊 [HIGH_PEAK] %s | Sessão: %s | L10: %.2f dB > %.2f dB | Local: %s | Sala: %s%n",
                        timeFormatted, sessionShort, l10, L10_THRESHOLD, location, classroom);
                    
                    Alert alert = new Alert(
                        Alert.AlertType.HIGH_PEAK,
                        sessionId,
                        timestamp,
                        location,
                        classroom,
                        Alert.Severity.HIGH,
                        String.format("Pico muito alto de som: %.2f dB (limite: %.2f dB)", l10, L10_THRESHOLD)
                    );
                    alerts.add(gson.toJson(alert));
                }
                
                // HIGH_BACKGROUND_NOISE
                if (l90 > L90_THRESHOLD) {
                    System.out.printf("🔊 [HIGH_BACKGROUND_NOISE] %s | Sessão: %s | L90: %.2f dB > %.2f dB | Local: %s | Sala: %s%n",
                        timeFormatted, sessionShort, l90, L90_THRESHOLD, location, classroom);
                    
                    Alert alert = new Alert(
                        Alert.AlertType.HIGH_BACKGROUND_NOISE,
                        sessionId,
                        timestamp,
                        location,
                        classroom,
                        Alert.Severity.MEDIUM,
                        String.format("Ruído de fundo excessivo: %.2f dB (limite: %.2f dB)", l90, L90_THRESHOLD)
                    );
                    alerts.add(gson.toJson(alert));
                }
                
                // HIGH_INTRUSION
                if (intrusion > INTRUSION_THRESHOLD) {
                    System.out.printf("🔊 [HIGH_INTRUSION] %s | Sessão: %s | Intrusão: %.2f dB > %.2f dB | L10: %.2f dB | L90: %.2f dB | Local: %s | Sala: %s%n",
                        timeFormatted, sessionShort, intrusion, INTRUSION_THRESHOLD, l10, l90, location, classroom);
                    
                    Alert alert = new Alert(
                        Alert.AlertType.HIGH_INTRUSION,
                        sessionId,
                        timestamp,
                        location,
                        classroom,
                        Alert.Severity.HIGH,
                        String.format("Intrusividade alta: variação de %.2f dB (limite: %.2f dB)", intrusion, INTRUSION_THRESHOLD)
                    );
                    alerts.add(gson.toJson(alert));
                }
                
                return alerts;
            })
            .selectKey((key, value) -> {
                try {
                    Alert alert = gson.fromJson(value, Alert.class);
                    return alert.getSessionId();
                } catch (Exception e) {
                    return key;
                }
            })
            .to(KafkaConfig.ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
    
    private static String formatTimestamp(String timestamp) {
        try {
            long ts = Long.parseLong(timestamp);
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(ts), 
                ZoneId.systemDefault()
            );
            return dateTime.format(formatter);
        } catch (Exception e) {
            return timestamp;
        }
    }
}