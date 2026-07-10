package com.example.processors;

import com.example.config.KafkaConfig;
import com.example.models.Alert;
import com.example.models.HeartData;
import com.example.serializers.JsonSerde;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TemporalCorrelationProcessor {

    private static final Gson gson = new Gson();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final int BPM_INCREASE_THRESHOLD = 10;

    private static final Duration WINDOW = Duration.ofSeconds(15); // 15s antes e 15s depois do alerta
    private static final Duration GRACE = Duration.ofSeconds(2);
    // Gap de inatividade: maior que o intervalo típico entre leituras de BPM,
    // para não fechar a sessão cedo demais entre uma leitura e outra.
    private static final Duration INACTIVITY_GAP = Duration.ofSeconds(10);

    public static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<Alert> alertSerde = new JsonSerde<>(Alert.class);
        Serde<HeartData> heartSerde = new JsonSerde<>(HeartData.class);
        Serde<AlertHeartPair> pairSerde = new JsonSerde<>(AlertHeartPair.class);
        Serde<BpmWindowSimple> windowSerde = new JsonSerde<>(BpmWindowSimple.class);

        KStream<String, Alert> parsedAlertStream = builder
            .stream(KafkaConfig.ALERTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(v -> safeParse(v, Alert.class))
            .filter((k, a) -> a != null && isEnvironmentalAlert(a.getType()));

        KStream<String, HeartData> parsedHeartStream = builder
            .stream("heart-monitoring", Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(v -> safeParse(v, HeartData.class))
            .filter((k, h) -> h != null);

        // 1. Join simétrico: cada alerta casa com CADA leitura de bpm em [-WINDOW, +WINDOW]
        KStream<String, AlertHeartPair> pairs = parsedAlertStream.join(
            parsedHeartStream,
            AlertHeartPair::new,
            JoinWindows.ofTimeDifferenceAndGrace(WINDOW, GRACE),
            StreamJoined.with(Serdes.String(), alertSerde, heartSerde)
        );

        // 2. Rechavear por instância do alerta (sessionId + timestamp = único)
        KStream<String, AlertHeartPair> rekeyed = pairs
            .selectKey((k, pair) -> pair.getAlert().getSessionId() + "|" + pair.getAlert().getTimestamp());

        // 3. SessionWindows ancora a janela no primeiro par que chega para aquela chave
        //    (na prática, próximo do timestamp do alerta), em vez de uma grade fixa de relógio.
        KTable<Windowed<String>, BpmWindowSimple> aggregated = rekeyed
            .groupByKey(Grouped.with(Serdes.String(), pairSerde))
            .windowedBy(SessionWindows.ofInactivityGapAndGrace(INACTIVITY_GAP, GRACE))
            .aggregate(
                BpmWindowSimple::new,
                (key, pair, agg) -> agg.addReading(pair),
                (key, agg1, agg2) -> agg1.merge(agg2), // exigido por SessionWindows: mescla sessões que se conectam
                Materialized.<String, BpmWindowSimple, SessionStore<Bytes, byte[]>>as("bpm-window-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(windowSerde)
            )
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // 4. Avaliar a correlação
        KStream<String, String> alertStream = aggregated
            .toStream()
            .mapValues((windowedKey, window) -> evaluateCorrelation(window))
            .filter((windowedKey, result) -> result != null)
            .selectKey((windowedKey, value) -> windowedKey.key().split("\\|")[0]);

        alertStream.to(KafkaConfig.ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String evaluateCorrelation(BpmWindowSimple window) {
        Alert alert = window.getAlert();
        List<Reading> readings = window.getReadings();
        if (alert == null || readings.isEmpty()) return null;

        readings.sort(Comparator.comparingLong(Reading::getTimestamp));
        long alertTs = parseTimestampToMillis(alert.getTimestamp());
        if (alertTs == 0L) return null;

        Reading peak = readings.stream().max(Comparator.comparingInt(Reading::getBpm)).orElse(null);
        Reading baseline = readings.stream()
            .filter(r -> r.getTimestamp() <= peak.getTimestamp())
            .min(Comparator.comparingInt(Reading::getBpm))
            .orElse(null);

        int bpmIncrease = peak.getBpm() - baseline.getBpm();
        if (bpmIncrease < BPM_INCREASE_THRESHOLD) return null;

        TemporalRelation relation;
        if (alertTs <= baseline.getTimestamp()) {
            relation = TemporalRelation.BEFORE_PEAK;
        } else if (alertTs <= peak.getTimestamp()) {
            relation = TemporalRelation.DURING_INCREASE;
        } else {
            relation = TemporalRelation.AFTER_PEAK;
        }
        if (relation == TemporalRelation.AFTER_PEAK) return null;

        Alert correlationAlert = new Alert(
            Alert.AlertType.STRESS_DETECTED,
            alert.getSessionId(),
            String.valueOf(System.currentTimeMillis()),
            alert.getLocation(),
            alert.getClassroom(),
            Alert.Severity.HIGH,
            String.format(
                "%s: %s → BPM %d→%d (%d leituras)",
                relation == TemporalRelation.BEFORE_PEAK ? "Antes do pico" : "Durante aumento",
                alert.getType(), baseline.getBpm(), peak.getBpm(), readings.size()
            )
        );

        System.out.printf("🧠 ALERTA GERADO [%s] | %s | BPM %d→%d | Leituras: %d%n",
            relation == TemporalRelation.BEFORE_PEAK ? "ANTES" : "DURANTE",
            alert.getType(), baseline.getBpm(), peak.getBpm(), readings.size()
        );

        return gson.toJson(correlationAlert);
    }

    private static <T> T safeParse(String json, Class<T> clazz) {
        try { return gson.fromJson(json, clazz); } catch (Exception e) { return null; }
    }

    private static boolean isEnvironmentalAlert(Alert.AlertType type) {
        return type == Alert.AlertType.HIGH_PEAK ||
               type == Alert.AlertType.HIGH_BACKGROUND_NOISE ||
               type == Alert.AlertType.HIGH_INTRUSION ||
               type == Alert.AlertType.CRITICAL_INTELLIGIBILITY;
    }

    public enum TemporalRelation { BEFORE_PEAK, DURING_INCREASE, AFTER_PEAK }

    public static class AlertHeartPair {
        private Alert alert;
        private HeartData heart;
        public AlertHeartPair() {}
        public AlertHeartPair(Alert alert, HeartData heart) { this.alert = alert; this.heart = heart; }
        public Alert getAlert() { return alert; }
        public HeartData getHeart() { return heart; }
    }

    public static class BpmWindowSimple {
        private Alert alert;
        private List<Reading> readings = new ArrayList<>();

        public BpmWindowSimple addReading(AlertHeartPair pair) {
            if (this.alert == null) this.alert = pair.getAlert();
            long ts = parseTimestampToMillis(pair.getHeart().getTimestamp());
            if (ts != 0L) readings.add(new Reading(ts, pair.getHeart().getBpm()));
            return this;
        }

        public BpmWindowSimple merge(BpmWindowSimple other) {
            if (this.alert == null) this.alert = other.alert;
            this.readings.addAll(other.readings);
            return this;
        }

        public Alert getAlert() { return alert; }
        public List<Reading> getReadings() { return readings; }
    }

    public static class Reading {
        private long timestamp;
        private int bpm;
        public Reading() {}
        public Reading(long timestamp, int bpm) { this.timestamp = timestamp; this.bpm = bpm; }
        public long getTimestamp() { return timestamp; }
        public int getBpm() { return bpm; }
    }

    private static long parseTimestampToMillis(String ts) {
        if (ts == null) return 0L;
        try {
            return Long.parseLong(ts);
        } catch (NumberFormatException e1) {
            try {
                return Instant.parse(ts).toEpochMilli();
            } catch (Exception e2) {
                return 0L;
            }
        }
    }
}