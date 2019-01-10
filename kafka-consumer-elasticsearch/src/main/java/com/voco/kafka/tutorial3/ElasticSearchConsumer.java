package com.voco.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * To run this, you need to:
 * 1. Have Zookeeper running
 * 2. Have Kafka server running
 * 3. (Optional) Have consumer running on CLI, kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets --group kafka-es-app
 * 4. Run TwitterProducer
 * 5. Run ElasticSearchConsumer
 * 6. Observe output
 */
public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {
        // Calling AWS Elasticsearch, and has to be public
        String hostname = "search-kafka-es-golltdwbt4be4lutskmdxvagoa.us-east-1.es.amazonaws.com";
        String username = "";
        String password = "";

        // Commented out because AWS ES is public, if it's not public, then add this in
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostname, 443, "https"));
        // Commented out because AWS ES is public, if it's not public, then add this in
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
//                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                });

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        return restHighLevelClient;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-es-app";
        // sample consumer CLI,  kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets --group kafka-es-app

        // 1. create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3. subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        RestHighLevelClient client = createClient();
//        logger.info("Got here1");
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
//        logger.info("Got here2");
        // 4. poll for new data
        while (true) {  // purely for demo purpose
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
//                logger.info("Got here3");
                // where we insert data into Elasticsearch
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets"
                ).source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info("Tweet id: " + id);

                try {
                    Thread.sleep(1000); // sleep to slowly watch
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close the client gracefully
//        client.close();
    }


}
