// web/AlertHistoryStore.java
package com.example.web;

import com.example.config.KafkaConfig;
import com.example.models.Alert;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Consome o tópico "alerts" continuamente e mantém em memória
 * um histórico de alertas agrupado por sessionId, para consulta pela API web.
 */
public class AlertHistoryStore {

    private static final Gson gson = new Gson();
    private static final int MAX_ALERTS_PER_SESSION = 500; // limite pra não crescer infinito em memória

    // sessionId -> lista de alertas (mais recente primeiro)
    private final Map<String, List<Alert>> alertsBySession = new ConcurrentHashMap<>();
    // sessionId -> metadata resumida (pra listar sessões rapidamente)
    private final Map<String, SessionSummary> sessionSummaries = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    public void start() {
        Thread consumerThread = new Thread(this::consumeLoop, "alert-history-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    public void stop() {
        running = false;
    }

    private void consumeLoop() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "alert-history-web-consumer"); // grupo próprio, não compete com outros
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // pega histórico desde o início

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(KafkaConfig.ALERTS_TOPIC));

            System.out.println("🌐 AlertHistoryStore: consumindo tópico '" + KafkaConfig.ALERTS_TOPIC + "'...");

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (var record : records) {
                    try {
                        Alert alert = gson.fromJson(record.value(), Alert.class);
                        if (alert == null || alert.getSessionId() == null) continue;
                        addAlert(alert);
                    } catch (Exception e) {
                        System.err.println("⚠️ AlertHistoryStore: erro ao parsear alerta: " + e.getMessage());
                    }
                }
            }
        }
    }

    private void addAlert(Alert alert) {
        String sessionId = alert.getSessionId();

        List<Alert> list = alertsBySession.computeIfAbsent(sessionId, k -> new CopyOnWriteArrayList<>());
        list.add(0, alert); // mais recente primeiro
        while (list.size() > MAX_ALERTS_PER_SESSION) {
            list.remove(list.size() - 1);
        }

        sessionSummaries.compute(sessionId, (id, existing) -> {
            SessionSummary summary = existing != null ? existing : new SessionSummary(sessionId);
            summary.alertCount++;
            summary.lastAlertMessage = alert.getMessage();
            summary.lastAlertType = alert.getType().name();
            summary.lastAlertTimestamp = alert.getTimestamp();
            if (alert.getLocation() != null && !"unknown".equals(alert.getLocation())) {
                summary.location = alert.getLocation();
            }
            if (alert.getClassroom() != null && !"unknown".equals(alert.getClassroom())) {
                summary.classroom = alert.getClassroom();
            }
            if (alert.getType() == Alert.AlertType.STRESS_DETECTED) {
                summary.stressDetectedCount++;
            }
            return summary;
        });
    }

    public List<SessionSummary> listSessions() {
        return new ArrayList<>(sessionSummaries.values());
    }

    public List<Alert> getAlertsForSession(String sessionId) {
        return alertsBySession.getOrDefault(sessionId, Collections.emptyList());
    }

    public SessionSummary getSessionSummary(String sessionId) {
        return sessionSummaries.get(sessionId);
    }

    public static class SessionSummary {
        public String sessionId;
        public String location = "unknown";
        public String classroom = "unknown";
        public int alertCount = 0;
        public int stressDetectedCount = 0;
        public String lastAlertType;
        public String lastAlertMessage;
        public String lastAlertTimestamp;

        public SessionSummary(String sessionId) {
            this.sessionId = sessionId;
        }
    }
}