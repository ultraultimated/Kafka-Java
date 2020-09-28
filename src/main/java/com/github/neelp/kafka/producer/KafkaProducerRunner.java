package com.github.neelp.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class KafkaProducerRunner {
    private static final String BOOTSTRAP_SEVER_CONFIG = "127.0.0.1:9092";

    public static void main(String[] args) {
        //create Properties for kafka producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SEVER_CONFIG);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "Message from the producer");

        //send data
        kafkaProducer.send(producerRecord);

        //flush the data and close the stream
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
