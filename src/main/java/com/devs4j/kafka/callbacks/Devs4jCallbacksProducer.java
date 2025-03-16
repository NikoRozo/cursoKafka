package com.devs4j.kafka.callbacks;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Devs4jCallbacksProducer {
    private static final Logger log = LoggerFactory.getLogger(Devs4jCallbacksProducer.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        Properties props=new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try(Producer<String, String> producer = new KafkaProducer<>(props)) {
            for(int i = 0; i< 10000; i++) {
                /*
                // Clases anonimas
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            log.error("There was an Error {} ", e.getMessage());
                        }
                        log.info("Partition: {}, Offset: {}, Topic: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic());
                    }
                });*/
                // Lambda - Logica sensilla
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"),
                    (recordMetadata, e) -> {;
                        if(e != null) {
                            log.error("There was an Error {} ", e.getMessage());
                        }
                        log.info("Partition: {}, Offset: {}, Topic: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic());
                    });
                /*
                // Clase Callback - Logica estructurada
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"), new Devs4jCallback());
                */
            }
            producer.flush();
        }

        log.info("Time: {} ", (System.currentTimeMillis() - startTime));
    }
}

