package org.example.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {
    private static String APP_NAME = "processor-app";
    private static String KAFKA_SERVER = "kafka-study:9092";
    private static String FROM_TOPIC = "stream_log";
    private static String TO_TOPIC = "stream_log_filter";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        topology.addSource("Source", FROM_TOPIC)
                .addProcessor("Process",
                        () -> new FilterProcessor(),
                        "SOURCE")
                .addSink("Sink",
                        TO_TOPIC,
                        "PROCESS");

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}
