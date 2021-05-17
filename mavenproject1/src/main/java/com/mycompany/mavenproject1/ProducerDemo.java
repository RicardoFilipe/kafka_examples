/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenproject1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author guest
 */
public class ProducerDemo{

    public static void main( String[] args )
    {
        String bootstrapservers = "localhost:9092";
        //Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer <String, String> producer = new KafkaProducer<String,String>(properties);

        //producer record
        ProducerRecord<String,String> record = new ProducerRecord<String,String>("new_topic","hello world2");
        //data async
        producer.send(record);
        
        producer.flush();
        
        //or flush and close
        producer.close();
    }
}
