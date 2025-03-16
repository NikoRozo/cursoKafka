package com.devs4j.kafka.transaccional;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TransactionalConsumer {
    private static final Logger log = LoggerFactory.getLogger(TransactionalConsumer.class);

    public static void main(String[] args) {
        Properties props=new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "devs4j-group");
        props.put("isolation.level", "read_committed");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval,ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("devs4j-topic"));
            while (true){
                ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(100));
                consumerRecord.forEach(record -> {
                    log.info("Offet = {}, Partition = {}, Key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                });
            }
        }
    }
}
