// consumers/AlertConsumer.java
package com.example.consumers;

import com.example.models.Alert;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class AlertConsumer {
    private static final Gson gson = new Gson();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final AtomicLong alertCounter = new AtomicLong(0);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.23.168.107:19092,172.23.168.107:29092,172.23.168.107:39092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("alerts"));
            
            System.out.println("📡 Aguardando alertas... (Ctrl+C para parar)");
            System.out.println("═".repeat(80));
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    continue;
                }
                
                for (var record : records) {
                    try {
                        Alert alert = gson.fromJson(record.value(), Alert.class);
                        long alertNumber = alertCounter.incrementAndGet();
                        
                        String timestamp = formatTimestamp(alert.getTimestamp());
                        String sessionShort = alert.getSessionId().substring(0, Math.min(8, alert.getSessionId().length()));
                        
                        // Formatar a saída com cores para melhor visualização
                        String alertType = formatAlertType(alert.getType());
                        String severity = formatSeverity(alert.getSeverity());
                        
                        System.out.printf("%s [%s] %s | Sessão: %s | %s | Local: %s | Sala: %s%n",
                            timestamp,
                            severity,
                            alertType,
                            sessionShort,
                            alert.getMessage(),
                            alert.getLocation(),
                            alert.getClassroom()
                        );
                        System.out.println("─".repeat(80));
                        
                    } catch (Exception e) {
                        System.err.println("❌ Erro ao processar alerta: " + e.getMessage());
                        System.err.println("   Mensagem: " + record.value());
                    }
                }
            }
        }
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
    
private static String formatAlertType(Alert.AlertType type) {
    switch (type) {
        case CRITICAL_INTELLIGIBILITY:
            return "🔴 INTELIGIBILIDADE CRÍTICA";
        case HIGH_PEAK:
            return "🔊 PICOS MUITO ALTO";
        case HIGH_BACKGROUND_NOISE:
            return "🔊 RUÍDO DE FUNDO EXCESSIVO";
        case HIGH_INTRUSION:
            return "⚡ INTRUSIVIDADE ALTA";
        case STRESS_DETECTED:
            return "🧠 ESTRESSE CORRELACIONADO (ALERTA + BPM)";
        default:
            return type.name();
    }
}
    
    private static String formatSeverity(Alert.Severity severity) {
        switch (severity) {
            case CRITICAL:
                return "🔥 CRÍTICO";
            case HIGH:
                return "🔴 ALTO";
            case MEDIUM:
                return "🟡 MÉDIO";
            case LOW:
                return "🟢 BAIXO";
            default:
                return severity.name();
        }
    }
}