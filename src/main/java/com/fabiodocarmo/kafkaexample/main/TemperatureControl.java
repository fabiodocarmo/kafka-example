package com.fabiodocarmo.kafkaexample.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TemperatureControl {
	
	static Logger logger = LoggerFactory.getLogger(TemperatureControl.class);

	public static void main(String[] args) {
		
		var consumer = new KafkaConsumer<String, String>(properties());
		
		consumer.subscribe(Collections.singletonList("teste"));

		while (true) {
			
			var records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> registro : records) {
				
				String msg = "Recebendo nova temperatura -----------> "
			    			+ " | key: " + registro.key() 
			    			+ " | value: " + registro.value();
				
				logger.info(msg);

				final String valor = registro.value().replaceAll("º", "");
				final Integer temperatura = Integer.valueOf(valor);
				
				if (temperatura > 30) {
					
					logger.info("Quente");
					
				} else if (temperatura < 20) {
					
					logger.info("Frio");
					
				}

				logger.info("Temperatura processada.");
			}
		}
	}

	private static Properties properties() {
		
		var properties = new Properties();
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, TemperatureControl.class.getName());
		
		return properties;
	}
}
