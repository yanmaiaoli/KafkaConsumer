// web/WebServer.java
package com.example.web;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class WebServer {

    private static final Gson gson = new Gson();

    private final AlertHistoryStore alertHistoryStore;
    private final KafkaStreams leqAverageStreams; // instância do LeqAverageProcessor, pra IQ no "leq-store"
    private final int port;

    public WebServer(AlertHistoryStore alertHistoryStore, KafkaStreams leqAverageStreams, int port) {
        this.alertHistoryStore = alertHistoryStore;
        this.leqAverageStreams = leqAverageStreams;
        this.port = port;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/api/sessions", this::handleListSessions);
        server.createContext("/api/sessions/", this::handleSessionDetail); // trailing slash = prefixo com {id}
        server.createContext("/", this::handleStatic);

        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
        server.start();
        System.out.println("🌐 WebServer rodando em http://localhost:" + port);
    }

    private void handleListSessions(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        List<AlertHistoryStore.SessionSummary> sessions = alertHistoryStore.listSessions();
        sendJson(exchange, 200, gson.toJson(sessions));
    }

    private void handleSessionDetail(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        String path = exchange.getRequestURI().getPath(); // /api/sessions/{sessionId}
        String sessionId = path.substring("/api/sessions/".length());
        if (sessionId.isEmpty()) {
            sendJson(exchange, 400, "{\"error\":\"sessionId ausente\"}");
            return;
        }

        AlertHistoryStore.SessionSummary summary = alertHistoryStore.getSessionSummary(sessionId);
        var alerts = alertHistoryStore.getAlertsForSession(sessionId);

        // Interactive Query no state store "leq-store" do LeqAverageProcessor.
        // Serializamos o objeto MultAVGs bruto via Gson, sem precisar conhecer seus campos exatos.
        Object averages = null;
        try {
            ReadOnlyKeyValueStore<String, Object> store = leqAverageStreams.store(
                StoreQueryParameters.fromNameAndType("leq-store", QueryableStoreTypes.keyValueStore())
            );
            averages = store.get(sessionId);
        } catch (Exception e) {
            System.err.println("⚠️ Erro na interactive query do leq-store: " + e.getMessage());
        }

        Map<String, Object> response = Map.of(
            "summary", summary != null ? summary : Map.of("sessionId", sessionId, "alertCount", 0),
            "alerts", alerts,
            "averages", averages != null ? averages : Map.of()
        );

        sendJson(exchange, 200, gson.toJson(response));
    }

    private void handleStatic(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/") || path.isEmpty()) path = "/index.html";

        String resourcePath = "web-static" + path; // arquivos em src/main/resources/web-static/
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            byte[] bytes = is.readAllBytes();
            String contentType = path.endsWith(".html") ? "text/html; charset=utf-8"
                : path.endsWith(".js") ? "application/javascript"
                : path.endsWith(".css") ? "text/css"
                : "application/octet-stream";
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
        }
    }

    private void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*"); // facilita testar localmente
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.getResponseBody().close();
    }
}