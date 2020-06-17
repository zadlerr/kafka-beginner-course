package kafka.tutorial2;

import com.google.common.collect.Lists;
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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final String consumerKey = "";
    private final String consumerSecret = "";
    private final String token = "";
    private final String secret = "";

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer(){

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("Setting Up");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        // create twitter client
        List myList = Lists.newArrayList("Columbia");
        Client hosebirdClient = createTwitterClient(msgQueue, myList);
        // Attempts to establish a connection.
        hosebirdClient.connect();

        // producer configs
        String bootstrap_servers = "127.0.0.1:9092";

        // create kafka producer
        KafkaProducer <String,String> producer = createKafkaProducer(bootstrap_servers);

        // use this in my shutdown()
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Application...");
            logger.info("Shutting down Twitter Client");
            hosebirdClient.stop();
            logger.info("Twitter Client Shutdown");
            logger.info("Closing Kafka Producer");
            producer.close();
            logger.info("Shutdown Hook Executed");
        }));



        int count = 0;
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }
            if (count > 100){
                hosebirdClient.stop();
                break;
            }
            if (msg != null){
                count++;
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null){
                            logger.error("Something went wrong", e);
                        }
                    }
                });
            }

        }
        logger.info("End of Application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue,List<String> searchTerms){

        // BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //        List<Long> followings = Lists.newArrayList(1234L, 566788L);
//        List<String> terms = Lists.newArrayList("searchTerms");
//        List<String> terms = searchTerms;
        //        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(searchTerms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("myTwitterClient")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(msg);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }

    private KafkaProducer<String,String> createKafkaProducer(String bootstrap_servers){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //making it a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // for Kafka >=2.0, otherwise use 1

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        return producer;
    }
}
