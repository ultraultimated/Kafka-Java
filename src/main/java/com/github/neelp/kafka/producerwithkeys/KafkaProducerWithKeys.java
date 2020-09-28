package com.github.neelp.kafka.producerwithkeys;

import com.github.neelp.kafka.producercallback.KafkaProducerCallback;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerWithKeys {
    private static final String BOOTSTRAP_SEVER_CONFIG = "127.0.0.1:9092";
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerCallback.class);

    public static void main(String[] args) {
        //create Properties for kafka producer
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SEVER_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            //create producer record
            String topic = "first_topic";
            String key = "id_" + i;
            String value = "Message Number" + i + " from the producer";
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(topic, key, value);
            logger.info("KEY: " + key);
            //send data
            Callback callback = new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("\nReceived Metadata \n" + "Topic: " + recordMetadata.topic() + "\n" +
                                "partition: " + recordMetadata.partition() + "\n" + "offset: " +
                                recordMetadata.offset() + "\nTimestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error in the callback", e);
                    }
                }
            };
            kafkaProducer.send(producerRecord, callback);
        }
        //flush the data and close the stream
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
