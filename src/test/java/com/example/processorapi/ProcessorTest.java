package com.example.processorapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorTest {

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private TopologyTestDriver testDriver;

    @BeforeEach
    public void beforeEach() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        new KafkaStreamsConfig().kStream(streamsBuilder);
        Topology topology = streamsBuilder.build();
        Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        testDriver = new TopologyTestDriver(topology, properties);

        Serdes.StringSerde stringSerde = new Serdes.StringSerde();
        inputTopic = testDriver.createInputTopic(
                "inputTopic", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
                "outputTopic", stringSerde.deserializer(), stringSerde.deserializer());
    }

    @AfterEach
    public void afterEach() {
        testDriver.close();
    }

    @Test
    void it_appends_hyphen_a_to_input_values() {
        inputTopic.pipeInput("key1", "value1");
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key1", "value1-a"));
    }

    @Test
    void it_populates_the_state_store() {
        inputTopic.pipeInput("key1", "value1");
        KeyValueStore<String, String> store = testDriver.getKeyValueStore("store");
        assertThat(store.get("key1")).isEqualTo("value1-a");
    }

    @Test
    void the_state_store_is_cleared_every_10_minutes() {
        inputTopic.pipeInput("key1", "value1");
        KeyValueStore<String, String> store = testDriver.getKeyValueStore("store");
        testDriver.advanceWallClockTime(Duration.ofMinutes(9));
        assertThat(store.all().hasNext()).isTrue();
        testDriver.advanceWallClockTime(Duration.ofMinutes(1));
        assertThat(store.all().hasNext()).isFalse();
    }

    @Test
    void clearing_the_state_store_propagates_a_tombstone_message_to_the_output_topic() {
        inputTopic.pipeInput("key1", "value1");
        testDriver.advanceWallClockTime(Duration.ofMinutes(10));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key1", "value1-a"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key1", null));
    }
}
