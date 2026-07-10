package com.example.processors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import com.example.models.HighAndLowsData;
import com.example.models.LeqData;
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
    private static KafkaProducer<String, String> producer;

    static {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producer = new KafkaProducer<>(producerProps);
    }
    
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
                        String time = json.has("timestamp") && json.get("timestamp").isJsonPrimitive()
                        ? json.get("timestamp").getAsString()
                        : String.valueOf(System.currentTimeMillis());

                        String sessionId = key;
                        String location = "unknown";
                        String classroom = "unknown";

                        if (json.has("metadata") && json.getAsJsonObject("metadata").has("spot")) {
                            JsonObject spot = json.getAsJsonObject("metadata").getAsJsonObject("spot");
                            if (spot.has("location") && spot.get("location").isJsonPrimitive()) {
                                location = spot.get("location").getAsString();
                            }
                            if (spot.has("classroom") && spot.get("classroom").isJsonPrimitive()) {
                                classroom = spot.get("classroom").getAsString();
                            }
                        }
                        
                        avgs.addValue(leq);
                        // ENVIO 1: Enviar LeqData
                        LeqData leqData = new LeqData(sessionId, time, location, classroom, leq);
                        sendToTopic(KafkaConfig.LEQ_TOPIC, sessionId, gson.toJson(leqData));
                        
                        // Processar L10 E L90 em um mesmo try/catch pois eles chegam alterados no mesmo evento
                        try {
                            if ((audioEvent.has("l10") && audioEvent.get("l10").isJsonPrimitive() && audioEvent.has("l90") && audioEvent.get("l90").isJsonPrimitive())) {
                                double l10 = audioEvent.get("l10").getAsDouble();
                                double l90 = audioEvent.get("l90").getAsDouble();
                                if (l10 > 0 && l90 > 0 && !Double.isNaN(l10) && !Double.isNaN(l90) && !Double.isInfinite(l10) && !Double.isInfinite(l90)) {
                                    avgs.addValuel10(l10);
                                    avgs.addValuel90(l90);
                                    HighAndLowsData highAndLowsData = new HighAndLowsData(sessionId, time, location, classroom, l10, l90);
                                    sendToTopic(KafkaConfig.HIGH_AND_LOWS_TOPIC, sessionId, gson.toJson(highAndLowsData));
                                    //logs
                                    System.out.printf("%s | SESSION %s | Leq avg: %.2f dB (based on %d samples) | L10: %.2f dB | L90: %.2f dB%n", 
                                        time, 
                                        key.substring(0, Math.min(8, key.length())),
                                        avgs.getAverage(),
                                        avgs.getCount(),
                                        avgs.getAveragel10(),
                                        avgs.getAveragel90()
                                    );
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error processing L10 and L90: " + e.getMessage());
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

        private static void sendToTopic(String topic, String key, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending to " + topic + ": " + exception.getMessage());
                } else {
                    /*System.out.printf("📤 Enviado para %s: key=%s, value=%s%n", 
                        topic, 
                        key.substring(0, Math.min(8, key.length())), 
                        value.substring(0, Math.min(50, value.length()))
                    );*/
                }
            });
        } catch (Exception e) {
            System.err.println("Error sending to topic: " + e.getMessage());
        }
    }
}