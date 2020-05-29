package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        System.out.println(logger.isInfoEnabled());

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for ( int i = 50; i < 60; i++){
            // producer record
            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("\nKey: " + key);
            // send data - async
            kafkaProducer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or exception is thrown
                    if (e == null) {
                        // successful send
                        logger.info("recieved new metadata.\n" +
                                "Topic: "+ recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
//                                "Key" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp() + "\n"
                                );

                    } else{
                        logger.error("Error while producing: " + e);
                    }

                }
            }).get(); // bad practice
        }
        //flush
        kafkaProducer.flush();

        //flush and close
//        kafkaProducer.close();
    }
}
