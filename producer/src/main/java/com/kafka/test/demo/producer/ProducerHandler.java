package com.kafka.test.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class ProducerHandler implements EventHandler{
	private Logger logger = LoggerFactory.getLogger(ProducerHandler.class.getSimpleName());	
	private KafkaProducer<String, String> producer;
	private String topic;
	
	public ProducerHandler(KafkaProducer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}

	@Override
	public void onOpen() throws Exception {
		// do nothing
	}

	@Override
	public void onClosed(){
		producer.close();
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		producer.send(new ProducerRecord<String, String>(topic, messageEvent.getData()));
		
		try {
			JsonObject object = new Gson().fromJson(messageEvent.getData(), JsonObject.class);
			logger.info("Data Entry added in the Kafka "+object.getAsJsonObject("meta").get("request_id"));
		}catch(JsonSyntaxException ex) {
			logger.info("JSON Syntax Error Occured : "+messageEvent.getData());
		}
	}

	@Override
	public void onComment(String comment) throws Exception {
		//do nothing
	}

	@Override
	public void onError(Throwable t) {
		logger.error("Error occurred: ", t);
	}

}
