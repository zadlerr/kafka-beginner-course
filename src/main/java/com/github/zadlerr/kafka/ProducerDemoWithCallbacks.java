package com.github.zadlerr.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);
        System.out.println(logger.isInfoEnabled());

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for ( int i = 0; i < 10; i++){
            // producer record
            ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","Hello World " + Integer.toString(i));

            // send data - async
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or exception is thrown
                    if (e == null) {
                        // successful send
                        logger.info("\nrecieved new metadata.\n" +
                                "Topic: "+ recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp() + "\n"
                                );
                    } else{
                        logger.error("Error while producing: " + e);
                    }

                }
            });
        }
        //flush
        kafkaProducer.flush();

        //flush and close
//        kafkaProducer.close();
    }
}
