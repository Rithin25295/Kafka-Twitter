package com.mytwitter.kafka.producer;

import com.google.common.collect.Lists;
import com.mytwitter.kafka.files.Resources;
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

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    List<String> terms = Lists.newArrayList("kafka", "java", "sport", "basketball", "NBA");

    public TwitterProducer() {
    }

    ;

    public static void main(String[] args) throws IOException {

        new TwitterProducer().run();

    }

    public void run() throws IOException {
        logger.info("Setup");


        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //Create a twitter
        // client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping the Application...");
            logger.info("Shutting down the client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");

        }));


        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = "Testing message";
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null)
                logger.info(msg);
            producer.send(new ProducerRecord<>("twit_tweet", null, msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("Something bad has happend");
                    }
                }
            });
        }
        logger.info("End of the Application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms


        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(Resources.getConsumerKey(),
                Resources.getConsumerSecret(),
                Resources.getToken(),
                Resources.getTokenSecret());

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServers = "127.0.0.1:9092";

        //create Producer Properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Safe Producer
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");//Kafka >=1.0. Use 1 otherwise

        //Create High-throughput Producer
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");//Kafka >=1.0. Use 1 otherwise
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");//Kafka >=1.0. Use 1 otherwise
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));//Kafka >=1.0. Use 1 otherwise


        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        return producer;

    }


}

