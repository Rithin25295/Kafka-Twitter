package com.mypractise;

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

public class ConsumerPractise {

    public static void main(String[] args) {


        Logger logger = LoggerFactory.getLogger(ConsumerPractise.class);
        //Create Consumer Configs

        String bootStrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //Create the Consumer
        KafkaConsumer<String,String> consumer =
                new KafkaConsumer<String, String>(prop);

        //Assign the Topic and Partition
        TopicPartition partitionToReadFrom = new TopicPartition(topic,2);
        Long offsetToReadFrom = 18L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek to the offset
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMessagesToRead =7;
        int numberOfMessagesRead = 0;
        boolean keepOnReadingFrom = true;

        //poll for new data
        while(keepOnReadingFrom)
        {
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records)
            {
                numberOfMessagesRead += 1;
                logger.info("Key : "+record.key()+" "+"Value : "+record.value()+" "+"Partition : "+
                        record.partition()+" "+"Offset : "+record.offset());
                if(numberOfMessagesRead >= numberOfMessagesToRead)
                {
                    keepOnReadingFrom = false;
                    break;
                }
            }
        }
    }
}
