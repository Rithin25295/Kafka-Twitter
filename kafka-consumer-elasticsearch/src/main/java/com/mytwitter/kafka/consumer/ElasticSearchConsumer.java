package com.mytwitter.kafka.consumer;

import com.google.gson.JsonParser;
import com.mytwitter.kafka.files.ConsumerResources;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {

        //Do not do if run on Local ElasticSearch
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(ConsumerResources.getUserName(), ConsumerResources.getPassword()));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(ConsumerResources.getHostName(), 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder
                        .HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String group_id = "kafka-twitter-twit";

        //Config properties
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerResources.getBootStrapServer());
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit of offsets
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;


    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweet) {
        return jsonParser.parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

        RestHighLevelClient client = createClient();


        KafkaConsumer<String, String> consumer = createConsumer("twit_tweet");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();


            for (ConsumerRecord<String, String> record : records) {

                //2 Strategies
                //First
                //Kafka-Generic ID
//                String id = record.topic()+"_"+record.partition()+"_"+record.offset();
                //Second
                //Twitter feed specific id

                try {
                    String id = extractIdFromTweet(record.value());
                    //Insert the data into elasticsearch

                    IndexRequest indexRequest = new IndexRequest("twit")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    bulkRequest.add(indexRequest); //add to the bulk request
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data : "+record.value());
                }

            }
            if(recordCount >0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committed offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed!!!");
                /*IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());*/
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

    //Close the client
    //client.close();



