package com.rithin.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeysWithCallback {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeysWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";
        //Create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "New message_"+Integer.toString(i);
            String key = "id_"+ Integer.toString(i);

            //Create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key,value);

            logger.info("Key : "+key);

            /*
            * id_0 - Partition 1
            * id_1 - Partition 0
            * id_2 - Partition 2
            * id_3 - Partition 0
            * id_4 - Partition 2
            * id_5 - Partition 2
            * id_6 - Partition 0
            * id_7 - Partition 2
            * id_8 - Partition 1
            * id_9 - Partition 2*/
            //Send Data - Asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new data : \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "TimeStamp : " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while running : ", e);
                    }
                }
            }).get();//blocks the .send() to make it synchronous - Do not do this in production!

            //Flush data
            producer.flush();


        }
        //Flush and Close
        producer.close();
    }
}

