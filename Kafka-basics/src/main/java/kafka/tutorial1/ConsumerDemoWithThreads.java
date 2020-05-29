package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-seventh-application";
        String autoOffset = "earliest"; // "earliest/latest"
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create consumer runnable
        logger.info("Creating Consumer Thread");
        Runnable consumerRunnable  = new ConsumerRunnable(latch,bootstrapServers,groupId,autoOffset,topic);

        Thread mythread = new Thread(consumerRunnable);

        mythread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }
    public class ConsumerRunnable implements Runnable{

        //instance variables
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private String bootstrapServers;
        private String groupId;
        private String autoOffset;
        private String topic;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        public ConsumerRunnable (CountDownLatch latch,
                               String bootstrapServers,
                               String groupId,
                               String autoOffset,
                               String topic){
            this.latch = latch;
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.autoOffset = autoOffset;
            this.topic = topic;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);

            //create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);
            //subscribe to topic
//            consumer.subscribe(Collections.singleton(topic)); // only subscribe to one topic
            consumer.subscribe(Arrays.asList("first_topic")); // multiple topics
        }
        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord rec : records) {
                        logger.info("Key: " + rec.key() + " Value: " + rec.value());
                        logger.info("Partition: " + rec.partition() + " Offset: " + rec.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received Shutdown Signal");
            }finally{
                consumer.close();
                // tell main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            // wakeup() special method to disrupt cosumer.poll()
            // throws WakeUpException
            consumer.wakeup();
        }
    }
}
