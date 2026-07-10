package com.example;

import com.example.config.KafkaConfig;
import com.example.processors.HighAndLowsAlertProcessor;
import com.example.processors.LeqAverageProcessor;
import com.example.processors.LeqAlertProcessor;
import com.example.processors.TemporalCorrelationProcessor;
import com.example.web.AlertHistoryStore;
import com.example.web.WebServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {

    public static void main(String[] args) throws Exception {
        KafkaStreams leqAverageStreams = buildAndStart(
            KafkaConfig.getStreamsProperties("leq-average-processor"),
            LeqAverageProcessor.buildTopology(),
            "📊 LeqAverageProcessor"
        );

        buildAndStart(
            KafkaConfig.getStreamsProperties("leq-alert-processor"),
            LeqAlertProcessor.buildTopology(),
            "🚨 LeqAlertProcessor"
        );

        buildAndStart(
            KafkaConfig.getStreamsProperties("high-and-lows-alert-processor"),
            HighAndLowsAlertProcessor.buildTopology(),
            "🔊 HighAndLowsAlertProcessor"
        );

        buildAndStart(
            KafkaConfig.getStreamsProperties("temporal-correlation-processor"),
            TemporalCorrelationProcessor.buildTopology(),
            "🧠 TemporalCorrelationProcessor"
        );

        // Consumer de alertas em memória, pra alimentar a API web
        AlertHistoryStore alertHistoryStore = new AlertHistoryStore();
        alertHistoryStore.start();

        // Servidor web: expõe /api/sessions e /api/sessions/{id}
        WebServer webServer = new WebServer(alertHistoryStore, leqAverageStreams, 8081);
        webServer.start();

        System.out.println("✅ Tudo rodando! Acesse http://localhost:8081");

        // Mantém o processo vivo
        new CountDownLatch(1).await();
    }

    private static KafkaStreams buildAndStart(Properties props, Topology topology, String name) {
        System.out.println(name + " Topology:\n" + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("💥 UNCAUGHT EXCEPTION em " + name + ": " + exception.getMessage());
            exception.printStackTrace();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        streams.setStateListener((newState, oldState) ->
            System.out.println("🔄 " + name + " | estado: " + oldState + " → " + newState));

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
        System.out.println("✅ " + name + " started!");
        return streams;
    }
}