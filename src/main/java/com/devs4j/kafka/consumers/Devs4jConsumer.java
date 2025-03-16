package com.devs4j.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Devs4jConsumer {
    private static final Logger log = LoggerFactory.getLogger(Devs4jConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "devs4j-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Commit se controla manualmente
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("devs4j-topic"));

            while (true){
                ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(100));
                consumerRecord.forEach(record -> {
                    log.info("Offet = {}, Partition = {}, Key = {}, value = {}", record.offset(), record.partition(), record.key(), record.value());
                    // consumer.commitSync(); // Commit manual, cuando requerimos controlar el commit de un mensaje en kafka
                });
            }
        }
    }
}
