package com.github.neelp.kafka.consumerwiththreads;

import com.github.neelp.kafka.consumer.KafkaConsumerBasic;
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

public class KafkaConsumerWithThreads {
    public KafkaConsumerWithThreads() {

    }

    public static void main(String[] args) {
        new KafkaConsumerWithThreads().run();

    }

    private void run() {
        String groupId = "my-second-application";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(KafkaConsumerBasic.class);
        String BOOTSTRAP_SEVER_CONFIG = "127.0.0.1:9092";

        //create properties for kafka consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SEVER_CONFIG);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        CountDownLatch latch = new CountDownLatch(1);
        Runnable runnable = new ConsumerThread(properties, topic, latch, logger);
        Thread myConsumerThread = new Thread(runnable);
        myConsumerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdownhook");
            ((ConsumerThread) runnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("Interruption occured ", e);
            } finally {
                logger.info("Application has exited");
            }
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Interruption occured", e);
        } finally {
            logger.info("Application is closing");
        }

    }
}

class ConsumerThread implements Runnable {
    private Properties properties;
    private String topic;
    private CountDownLatch latch;
    private Logger logger;
    private KafkaConsumer<String, String> consumer;

    public ConsumerThread(Properties properties, String topic, CountDownLatch latch, Logger logger) {
        this.properties = properties;
        this.topic = topic;
        this.latch = latch;
        this.logger = logger;
        consumer = new KafkaConsumer<String, String>(this.properties);
        //Subscribe to topics
        consumer.subscribe(Arrays.asList(this.topic));// could be a list of topics
    }

    @Override
    public void run() {
        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("KEY: " + record.key() + "\t" + "VALUE: " + record.value() +
                            "\t OFFSET: " + record.offset() + "\t PARTITION: " + record.partition());
                }
            }
        } catch (WakeupException e) {
            logger.error("Received shutdown signal", e);
        } finally {
            consumer.close();
            latch.countDown();
        }

    }

    public void shutdown() {
        consumer.wakeup();
    }
}
