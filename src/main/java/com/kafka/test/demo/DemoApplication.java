package com.kafka.test.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import com.kafka.test.demo.consumer.OpenSearchConsumer;
import com.kafka.test.demo.producer.WikimediaProducer;

@SpringBootApplication
public class DemoApplication {
	private Logger logger = LoggerFactory.getLogger(DemoApplication.class.getSimpleName());
	
	@Value("${resourceUrl}")
	private String resourceDirUrl;

	@Value("${dockerConfigFile}")
	private String dockerConfig;
	
	@Value("${dockerStartFile}")
	private String dockerStartFile;
	
	@Value("${dockerEndFile}")
	private String dockerEndFile;
	
	private String WORKING_DIR;
	
	@Autowired
	private WikimediaProducer producer;
	
	@Autowired
	private OpenSearchConsumer consumer;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}
	
	@EventListener(ApplicationReadyEvent.class)
	public void triggerKafkaEvent() throws IOException {		
		try {
			WORKING_DIR = new File(resourceDirUrl).getAbsolutePath();
			runBatchCommands(dockerStartFile);
			logger.info("Kafka instance is up");
			
			producer.runProducer();
			logger.info("Producer has finished inserting the data into Kafka");
			
			consumer.runConsumer();
			logger.info("Consumer has finished reading the data from Kafka");
			
			runBatchCommands(dockerEndFile);
			logger.info("Kafka instance is down");
		}catch(WakeupException ex) {
			logger.info("Kafka System is shutting down.");
		}finally {
			logger.info("Kafka System is shut down.");
		}
	}
	
	private void runBatchCommands(String fileName) {
		try (BufferedReader buffer = new BufferedReader(new FileReader(WORKING_DIR+fileName))) {
			String str;
			while ((str = buffer.readLine()) != null) {
				logger.info("Command Executed : "+str.replace("{DIR}", WORKING_DIR+dockerConfig));
				Runtime.getRuntime().exec(str.replace("{DIR}", WORKING_DIR+dockerConfig));
			}
		}
		catch (IOException e) {
			logger.error("Error Occurred : "+e.getMessage());
		}
	}
}
