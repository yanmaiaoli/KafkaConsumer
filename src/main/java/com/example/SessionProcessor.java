package com.example;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class SessionProcessor {
    
    private static final Gson gson = new Gson();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String BROKER_IP = "172.23.168.107";
    
    private static final Map<String, SessionState> activeSessions = new ConcurrentHashMap<>();
    
    static class SessionState {
        String sessionId;
        String location;
        String classroom;
        String observer;
        String startTime;
        String endTime;
        
        List<Double> l90Window = new ArrayList<>();
        List<Double> laeqWindow = new ArrayList<>();
        List<Double> intrusionWindow = new ArrayList<>();
        
        // Acumuladores para médias
        double sumL10 = 0;
        double sumL90 = 0;
        double sumLAeq = 0;
        
        int l10Count = 0;
        int l90Count = 0;
        int laeqCount = 0;
        
        double lastL10 = 0;
        double lastL90 = 0;
        
        SessionState(String sessionId, String location, String classroom, String observer, String startTime) {
            this.sessionId = sessionId;
            this.location = location;
            this.classroom = classroom;
            this.observer = observer;
            this.startTime = startTime;
        }
        
        double getAvgL10() {
            return l10Count > 0 ? sumL10 / l10Count : 0;
        }
        
        double getAvgL90() {
            return l90Count > 0 ? sumL90 / l90Count : 0;
        }
        
        double getAvgLAeq() {
            return laeqCount > 0 ? sumLAeq / laeqCount : 0;
        }
        
        double getAvgIntrusion() {
            return getAvgL10() - getAvgL90();
        }
        
        // Verifica se o valor deve ser considerado para o cálculo
        boolean shouldConsiderL10(double l10) {
            return l10 > 0 && l10 != lastL10;
        }
        
        boolean shouldConsiderL90(double l90) {
            return l90 > 0 && l90 != lastL90;
        }
        
        void updateLastValues(double l10, double l90) {
            if (l10 != 0 && l10 != lastL10) {
                lastL10 = l10;
            }
            if (l90 != 0 && l90 != lastL90) {
                lastL90 = l90;
            }
        }
    }
    
    public static void main(String[] args) {
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
            String.format("%s:19092,%s:29092,%s:39092", BROKER_IP, BROKER_IP, BROKER_IP));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "session-processor-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        System.out.println("============================================================");
        System.out.println("SESSION PROCESSOR - Kafka Consumer");
        System.out.println("============================================================");
        System.out.println("Connected to: " + BROKER_IP + ":19092,29092,39092");
        System.out.println("Topic: audio-monitoring");
        System.out.println("============================================================\n");
        System.out.println("Waiting for sessions...(consumidorrrrrrrrrrrrrrrrrrrrrrrrrrr)\n");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("audio-monitoring"));
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach((record) -> {
                    String sessionId = record.key();
                    String value = record.value();
                    long timestamp = System.currentTimeMillis();
                    
                    try {
                        JsonObject json = gson.fromJson(value, JsonObject.class);
                        
                        if (json.has("startTime") && !json.has("audioEvent")) {
                            handleSessionStart(sessionId, json);
                        }
                        else if (json.has("audioEvent")) {
                            handleAudioEvent(sessionId, json, timestamp);
                        }
                        else if (json.has("endTime")) {
                            handleSessionEnd(sessionId, json);
                        }
                        
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                    }
                });
            }   
        }
    }
    
 private static void handleSessionStart(String sessionId, JsonObject json) {
        JsonObject metadata = json.getAsJsonObject("metadata");
        JsonObject spot = metadata.getAsJsonObject("spot");
        
        String location = spot.get("location").getAsString();
        String classroom = spot.get("classroom").getAsString();
        String observer = metadata.get("observer").getAsString();
        String startTime = metadata.get("startTime").getAsString();
        
        SessionState state = new SessionState(sessionId, location, classroom, observer, startTime);
        activeSessions.put(sessionId, state);
        
        System.out.println("SESSION START | " + getCurrentTime() + " | " + sessionId.substring(0, 8) + 
                         " | " + location + " - " + classroom);
    }
    
private static void handleAudioEvent(String sessionId, JsonObject json, long timestamp) {
    SessionState state = activeSessions.get(sessionId);
    if (state == null) return;
    
    JsonObject audioEvent = json.getAsJsonObject("audioEvent");
    double lpi = audioEvent.get("lpi").getAsDouble();
    double l10 = audioEvent.has("l10") ? audioEvent.get("l10").getAsDouble() : 0;
    double l90 = audioEvent.has("l90") ? audioEvent.get("l90").getAsDouble() : 0;
    
    // Usando LPI como aproximação do LAeq
    double laeq = lpi;
    
    // Acumula L10 quando válido e adiciona à janela de alertas
    if (state.shouldConsiderL10(l10)) {
        state.sumL10 += l10;
        state.l10Count++;
        System.out.println("DEBUG | L10 valid | value: " + String.format("%.1f", l10) + " | count: " + state.l10Count);
        
        // Alerta 4: Pico muito alto (L10 > 80 dB)
        if (l10 > 80) {
            System.out.println("INFO | High peak detected | L10: " + 
                             String.format("%.1f", l10) + " dB | Location: " + 
                             state.location + " - " + state.classroom);
        }
    }
    
    // Acumula L90 quando válido e adiciona à janela de alertas
    if (state.shouldConsiderL90(l90)) {
        state.sumL90 += l90;
        state.l90Count++;
        System.out.println("DEBUG | L90 valid | value: " + String.format("%.1f", l90) + " | count: " + state.l90Count);
        
        // Adiciona às janelas para alertas (apenas valores válidos)
        state.l90Window.add(l90);
        
        // ALERTA 1: Ruído de fundo excessivo (média do L90 > 35 dB)
        if (state.l90Window.size() >= 2) {
            double avgL90 = state.l90Window.stream()
                .mapToDouble((value) -> value.doubleValue())
                .average()
                .orElse(0);
            
            if (avgL90 > 35) {
                System.out.println("WARNING | Background noise | L90 avg: " + 
                                 String.format("%.1f", avgL90) + " dB | Location: " + 
                                 state.location + " - " + state.classroom);
            }
            
            // Mantém apenas as últimas 2 amostras
            if (state.l90Window.size() > 2) state.l90Window.remove(0);
        }
    }
    
    // Acumula LAeq (cada evento, pois LPI vem sempre)
    state.sumLAeq += laeq;
    state.laeqCount++;
    
    // Adiciona à janela de alertas do LAeq (todos os eventos)
    state.laeqWindow.add(laeq);
    
    // ALERTA 2: Inteligibilidade crítica (média do LAeq > 65 dB)
    if (state.laeqWindow.size() >= 3) {
        double avgLAeq = state.laeqWindow.stream()
            .mapToDouble((value) -> value.doubleValue())
            .average()
            .orElse(0);
        
        if (avgLAeq > 65) {
            System.out.println("CRITICAL | Speech intelligibility compromised | LAeq avg: " + 
                             String.format("%.1f", avgLAeq) + " dB | Location: " + 
                             state.location + " - " + state.classroom);
        }
        
        // Mantém apenas as últimas 3 amostras
        if (state.laeqWindow.size() > 3) state.laeqWindow.remove(0);
    }
    
    // Calcula intrusão apenas quando ambos os valores são válidos
    if (l10 != 0 && l90 != 0 && state.shouldConsiderL10(l10) && state.shouldConsiderL90(l90)) {
        double currentIntrusion = l10 - l90;
        state.intrusionWindow.add(currentIntrusion);
        
        // ALERTA 3: Intrusividade alta (variação pico-fundo > 20 dB)
        if (state.intrusionWindow.size() >= 2) {
            double avgIntrusion = state.intrusionWindow.stream()
                .mapToDouble((value) -> value.doubleValue())
                .average()
                .orElse(0);
            
            if (avgIntrusion > 20) {
                System.out.println("WARNING | High intrusiveness | Variation (L10-L90): " + 
                                 String.format("%.1f", avgIntrusion) + " dB | Location: " + 
                                 state.location + " - " + state.classroom);
            }
            
            // Mantém apenas as últimas 2 amostras
            if (state.intrusionWindow.size() > 2) state.intrusionWindow.remove(0);
        }
    }
    
    state.updateLastValues(l10, l90);
    
    // Print periódico para acompanhamento
    if (state.laeqCount % 10 == 0) {
        System.out.println("STATS | " + getCurrentTime() + " | " + state.sessionId.substring(0, 8) + 
                         " | L10 samples: " + state.l10Count + " (avg: " + String.format("%.1f", state.getAvgL10()) + ")" +
                         " | L90 samples: " + state.l90Count + " (avg: " + String.format("%.1f", state.getAvgL90()) + ")" +
                         " | LAeq samples: " + state.laeqCount + " (avg: " + String.format("%.1f", state.getAvgLAeq()) + ")" +
                         " | Intrusion: " + String.format("%.1f", state.getAvgIntrusion()));
    }
}
    
    private static void handleSessionEnd(String sessionId, JsonObject json) {
        SessionState state = activeSessions.remove(sessionId);
        if (state == null) return;
        
        String endTime = json.get("endTime").getAsString();
        state.endTime = endTime;
        
        System.out.println("\n============================================================");
        System.out.println("SESSION END | " + sessionId.substring(0, 8));
        System.out.println("============================================================");
        System.out.println("Location: " + state.location + " - " + state.classroom);
        System.out.println("Observer: " + state.observer);
        System.out.println("Start: " + state.startTime);
        System.out.println("End: " + state.endTime);
        System.out.println();
        System.out.println("STATISTICS:");
        System.out.println("  L10 avg: " + String.format("%.1f", state.getAvgL10()) + " dB (based on " + state.l10Count + " samples)");
        System.out.println("  L90 avg: " + String.format("%.1f", state.getAvgL90()) + " dB (based on " + state.l90Count + " samples)");
        System.out.println("  Intrusion (L10-L90): " + String.format("%.1f", state.getAvgIntrusion()) + " dB");
        System.out.println("  LAeq avg: " + String.format("%.1f", state.getAvgLAeq()) + " dB (based on " + state.laeqCount + " samples)");
        System.out.println("============================================================\n");
    }
    
    private static String getCurrentTime() {
        return LocalDateTime.now().format(formatter);
    }
}