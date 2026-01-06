package com.kafka.test.demo.producer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

@Component
public class WikimediaProducer {
	private Logger logger = LoggerFactory.getLogger(WikimediaProducer.class.getSimpleName());
	
	@Value("${server}")
	private String server;
	
	@Value("${producerPort}")
	private String producerPort;
	
	@Value("${topic}")
	private String topic;
	
	@Value("${url}")
	private String url;
	
	private KafkaProducer<String, String> producer;
	private EventSource event;
	
	public void runProducer() {
		String bootstrapServer = server+":"+producerPort;
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		producer = new KafkaProducer<>(properties);
		
		EventHandler handler = new ProducerHandler(producer, topic);
		EventSource.Builder builder = new EventSource.Builder(handler, URI.create(url));
		event = builder.build();
		
		event.start();
		
		try {
			TimeUnit.SECONDS.sleep(30);
		} catch (InterruptedException e) {
			logger.error("Error Occurred : "+e.getMessage());
		}
		
		event.close();
	}
}
