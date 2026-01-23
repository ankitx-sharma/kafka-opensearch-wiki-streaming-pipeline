package com.kafka.test.demo.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ProducerRunner implements ApplicationRunner {

	@Autowired
	private WikimediaProducer producer;
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		producer.runProducer();
	}

}
