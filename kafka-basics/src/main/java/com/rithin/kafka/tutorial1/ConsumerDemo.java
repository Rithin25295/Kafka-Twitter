package com.rithin.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootStrapServers = "127.0.0.1:9092";
        String group_id = "my-fourth-app";
        String topic = "first_topic";

        //Create Consumer configs
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);

        //Subscribe the consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while(true)
        {
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100)); //new from kafka 2.0.0
            for(ConsumerRecord<String,String> record : records)
            {
                logger.info("Key : "+ record.key()+"  "+ "Value : "+record.value()+"\n"+
                        "Partition : "+record.partition()+"\n"+
                        "Offset : "+record.offset());

            }


        }



    }
}
