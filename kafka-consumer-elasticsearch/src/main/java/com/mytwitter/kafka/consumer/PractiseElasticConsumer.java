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
import static org.elasticsearch.client.RestClient.builder;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PractiseElasticConsumer

{
    //Create Client function
    public static RestHighLevelClient createClient()

    {
        //Provide Credentials
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(ConsumerResources.getUserName(),ConsumerResources.getPassword()));

        //Specify Host
        RestClientBuilder builder = builder(
                new HttpHost(ConsumerResources.getHostName(),443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });


        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    //Create Consumer Function with topic parameter
    public static KafkaConsumer<String,String> createConsumer(String group_id,String topic)

    {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerResources.getBootStrapServer());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        //properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        //properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");


        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    //Extract id
    private static JsonParser jsonParser = new JsonParser();

    private static String extractId(String tweet)
    {
        return jsonParser.parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }


    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(PractiseElasticConsumer.class);

        RestHighLevelClient client = createClient();

        KafkaConsumer<String,String> consumer = createConsumer("kafka-demo-elasticsearch-practise-1",
                "twitter_tweets");

        while(true)
        {
            //poll the records
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received "+records.count() + " records");
            for(ConsumerRecord<String,String> record : records)
            {
                String id = extractId(record.value());

                //Send the data
                IndexRequest indexRequest =
                        new IndexRequest("twitter","tweets",id)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info("Id - "+id);

                //Wait hold for the efficiency
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            /*logger.info("Committing offsets...");
            consumer.commitSync();
            logger.info("Offsets have been committed!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }

        /*client.close();*/



    }







}
