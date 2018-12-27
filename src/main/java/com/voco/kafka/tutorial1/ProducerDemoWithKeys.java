package com.voco.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        // 1. create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            // 3. create a producer record
            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            logger.info("Key: " + key);
            // id_0 / P1
            // id_1 / P0
            // id_2 / P2
            // id_3 / P0
            // id_4 / P2
            // id_5 / P2
            // id_6 / P0
            // id_7 / P2
            // id_8 / P1
            // id_9 / P2
            // 4. send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.offset());
                        // the record was successfully sent
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();   // block the .send() to make it synchronous - DON'T DO THIS IN PROD

            producer.flush();   // need to flush data for us to wait for it
        }
        producer.close();   // close producer
    }
}
