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
            }
        }
    }

    public Client createTwitterEvent(BlockingQueue<String> blockingQueue) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint statusesFilterEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("bitcoin");
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
}
