package com.example.processorapi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ProcessorTest {

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    public void beforeEach() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        new KafkaStreamsConfig().kStream(streamsBuilder);
        Topology topology = streamsBuilder.build();
        Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties);

        Serdes.StringSerde stringSerde = new Serdes.StringSerde();
        inputTopic = testDriver.createInputTopic(
                "inputTopic", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
                "outputTopic", stringSerde.deserializer(), stringSerde.deserializer());
    }

    @Test
    void it_appends_hyphen_a_to_input_values() {
        inputTopic.pipeInput("key1", "value1");
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key1", "value1-a"));
    }
}
