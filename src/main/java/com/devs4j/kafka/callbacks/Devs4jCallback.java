package com.devs4j.kafka.callbacks;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Devs4jCallback implements Callback {

    private static final Logger log = LoggerFactory.getLogger(Devs4jCallback.class);
    
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null) {
            log.error("There was an Error {} ", e.getMessage());
        }
        log.info("Partition: {}, Offset: {}, Topic: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.topic());
    }
}
