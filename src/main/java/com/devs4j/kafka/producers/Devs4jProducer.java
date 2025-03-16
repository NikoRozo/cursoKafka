package com.devs4j.kafka.producers;

import com.devs4j.kafka.consumers.Devs4jConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Devs4jProducer {
    private static final Logger log = LoggerFactory.getLogger(Devs4jProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties props=new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try(Producer<String, String> producer = new KafkaProducer<>(props)) {
            for(int i = 0; i< 10000000; i++) {
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"));
            }
            producer.flush();
        }

        log.info("Time: {} ", (System.currentTimeMillis() - startTime));
    }
}
