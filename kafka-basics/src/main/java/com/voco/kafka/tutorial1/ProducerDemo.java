package com.voco.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // 1. create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3. create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");

        // 4. send data - asynchronous
        producer.send(record);

        producer.flush();   // need to flush data for us to wait for it

        producer.close();   // close producer
    }
}
