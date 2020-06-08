package com.rithin.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootStrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        //Create Consumer configs
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);

        //Assign and Seek are mostly used to replay the data or fetch a specific message

        //Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesRead = 0;

        //poll for new data
        while(keepOnReading)
        {
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100)); //new from kafka 2.0.0
            for(ConsumerRecord<String,String> record : records)
            {
                numberOfMessagesRead += 1;
                logger.info("Key : "+ record.key()+"  "+ "Value : "+record.value()+"\n"+
                        "Partition : "+record.partition()+"\n"+
                        "Offset : "+record.offset());
                if(numberOfMessagesRead>=numberOfMessagesToRead)
                {
                    keepOnReading = false; //To exit the while loop
                    break; //To exit the for loop
                }

            }


        }
        logger.info("Exiting the application");



    }
}
