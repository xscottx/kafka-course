package com.voco.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private Properties properties = new Properties();
    private List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer () throws IOException {
        String propFileName = "config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            properties.load(inputStream);
            System.out.println(properties);
        } else {
            throw new FileNotFoundException("Property file '" + propFileName + "' not found in the classpath");
        }
    }

    public static void main(String[] args) throws IOException {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<>(100000);

        // create a twitter client
        Client client = createTwitterEvent(blockingQueue);
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client from Twitter...");
            client.stop();
            logger.info("Shutting down Kafka Producer...");
            kafkaProducer.close();
            logger.info("Done.");
        }));

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = blockingQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info("msg: " + msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
    }

    public Client createTwitterEvent(BlockingQueue<String> blockingQueue) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint statusesFilterEndpoint = new StatusesFilterEndpoint();

        statusesFilterEndpoint.trackTerms(terms);

        Authentication authentication = new OAuth1(properties.getProperty("consumerKey"), properties.getProperty("consumerSecret"),
                properties.getProperty("token"), properties.getProperty("tokenSecret"));

        ClientBuilder builder = new ClientBuilder()
                .name("kafka-vo-client-01")
                .hosts(hosts)
                .authentication(authentication)
                .endpoint(statusesFilterEndpoint)
                .processor(new StringDelimitedProcessor(blockingQueue));

        Client client = builder.build();
        return client;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        // 1. create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2a. create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");  // kafka 2.0 >= 1.1

        // 2. create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        return producer;
    }
}
