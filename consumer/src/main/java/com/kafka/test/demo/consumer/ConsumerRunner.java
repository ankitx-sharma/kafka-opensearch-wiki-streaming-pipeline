package com.kafka.test.demo.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ConsumerRunner implements ApplicationRunner {

	@Autowired
	private OpenSearchConsumer openSearchConsumer;
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		openSearchConsumer.runConsumer();
	}

}
