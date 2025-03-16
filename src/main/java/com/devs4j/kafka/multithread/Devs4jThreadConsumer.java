package com.devs4j.kafka.multithread;

import com.devs4j.kafka.consumers.Devs4jConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class Devs4jThreadConsumer extends Thread {

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private static final Logger log = LoggerFactory.getLogger(Devs4jThreadConsumer.class);

    public Devs4jThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("devs4j-topic"));

        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(100));
                consumerRecord.forEach(record -> {
                    log.debug("Offet = {}, Partition = {}, Key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                    if((Integer.parseInt(record.key()) % 100000) == 0){
                        log.info("Offet = {}, Partition = {}, Key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                    }
                });
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }

    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
