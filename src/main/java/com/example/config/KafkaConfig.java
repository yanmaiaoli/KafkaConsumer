package com.example.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import java.util.Properties;

public class KafkaConfig {
    
    private static final String BOOTSTRAP_SERVERS = "172.23.168.107:19092,172.23.168.107:29092,172.23.168.107:39092";
    public static final String AUDIO_TOPIC = "audio-monitoring";
    public static final String LEQ_ALERT_TOPIC = "alert_leq";
    public static final String HIGH_AND_LOWS_TOPIC = "alert_high_and_lows";
    
    public static Properties getStreamsProperties(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}