package com.fabiodocarmo.kafkaexample.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SensorMain {
	
    public static void main(String[] args) throws ExecutionException, InterruptedException {
    	
    	Logger logger = LoggerFactory.getLogger(SensorMain.class);

        var producer = new KafkaProducer<String, String>(properties());
        var key = "TEMPERATURA";
        var value = "34º";
        var prducerRecord = new ProducerRecord<String, String>("teste", key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            
            String msg = "Mensagem enviada com sucesso para: " + data.topic() 
            			+ " | partition " + data.partition() 
            			+ " | offset " + data.offset() 
            			+ " | tempo " + data.timestamp();
            
			logger.info(msg);
        };
        
        producer.send(prducerRecord, callback).get();
    }

    private static Properties properties() {
    	
        var properties = new Properties();
        
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        return properties;
        
    }
}
