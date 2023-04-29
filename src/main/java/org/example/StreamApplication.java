package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamApplication {
    private static String APP_NAME = "stream-app";
    private static String KAFKA_SERVER = "kafka-study:9092";
    private static String FROM_TOPIC = "stream_log";
    private static String TO_TOPIC = "stream_log_copy";
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(FROM_TOPIC);
        streamLog.to(TO_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
