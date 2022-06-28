package com.example.processorapi;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class ClearStoreProcessor<K, V> implements Processor<K, V, Void, Void> {
    private final String storeName;
    private KeyValueStore<K, V> store;

    public ClearStoreProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        store = context.getStateStore(storeName);
        context.schedule(
                Duration.ofMinutes(10),
                PunctuationType.WALL_CLOCK_TIME,
                timestamp -> store.all().forEachRemaining(keyValue -> store.delete(keyValue.key)));
    }

    @Override
    public void process(Record<K, V> record) {

    }
}
