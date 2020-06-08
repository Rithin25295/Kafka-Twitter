package com.rithin.kafka.streams;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets

{

    public static void main(String[] args)

    {
        //create properties

        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input Topic
        KStream<String,String> inputTopic = streamsBuilder.stream("twit_tweet");
        KStream<String,String> filteredStream = inputTopic.filter(
                //filter for tweets which has users of over 10000 followers
                (k,jsonTweet) -> extractUserFromTweets(jsonTweet) >10000
        );
        filteredStream.to("important_tweets");

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                prop
        );

        //Start our streams application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();
    private static Integer extractUserFromTweets(String jsonTweet)
    {
        try {
            return jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
