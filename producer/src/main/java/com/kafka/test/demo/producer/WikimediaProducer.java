package com.kafka.test.demo.producer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

import okhttp3.Headers;

@Component
public class WikimediaProducer {
	private Logger logger = LoggerFactory.getLogger(WikimediaProducer.class.getSimpleName());
	
	@Value("${kafka.bootstrap}")
	private String server;
	
	@Value("${kafka.topic}")
	private String topic;
	
	@Value("${wikimedia.url}")
	private String url;
	
	@Value("${producer.runSeconds}")
	private String runSeconds;
	
	@Value("${wikimedia.userAgent}")
	private String userAgent;
	
	private KafkaProducer<String, String> producer;
	private EventSource event;
	
	public void runProducer() {
	
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		producer = new KafkaProducer<>(properties);
		
		CountDownLatch done = new CountDownLatch(1);
		EventHandler handler = new ProducerHandler(producer, topic);
		
		Headers headers = new Headers.Builder()
							.add("User-Agent", userAgent)
							.build();
		EventSource.Builder builder = new EventSource.Builder(handler, URI.create(url)).headers(headers);
		event = builder.build();
		
		event.start();
		
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.schedule(() -> {
			event.close();
			done.countDown();
		}, Integer.parseInt(runSeconds), TimeUnit.SECONDS);
		
		try {
			done.await();
		} catch (InterruptedException e) {
			logger.error("Error Occurred : "+e.getMessage());
		}
		executor.shutdown();
		producer.flush();
		producer.close();
	}
}
