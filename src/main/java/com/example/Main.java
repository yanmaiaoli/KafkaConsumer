package com.example;

import com.example.config.KafkaConfig;
import com.example.processors.LeqAverageProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {
    
    public static void main(String[] args) {
        // Executar o processador de médias
        runLeqAverageProcessor();
    }
    
    private static void runLeqAverageProcessor() {
        Properties props = KafkaConfig.getStreamsProperties("leq-average-processor");
        Topology topology = LeqAverageProcessor.buildTopology();
        
        System.out.println("Topology:\n" + topology.describe());
        
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
        
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}