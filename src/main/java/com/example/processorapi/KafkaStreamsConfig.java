package com.example.processorapi;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.time.Duration;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "testStreams",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        ));
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder.stream("inputTopic");

        stream.mapValues(v -> v + "-a")
                .toTable(Materialized.as("store"))
                .toStream()
                .to("outputTopic");

        stream.process(() -> new Processor<>() {
            private KeyValueStore<String, String> store;

            @Override
            public void init(ProcessorContext<Void, Void> context) {
                store = context.getStateStore("store");
                context.schedule(
                        Duration.ofMinutes(10),
                        PunctuationType.WALL_CLOCK_TIME,
                        timestamp -> store.all().forEachRemaining(keyValue -> store.delete(keyValue.key)));
            }

            @Override
            public void process(Record<String, String> record) {

            }
        }, "store");

        return stream;
    }
}
