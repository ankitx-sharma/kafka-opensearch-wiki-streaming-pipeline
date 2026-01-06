package com.kafka.test.demo.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class OpenSearchConsumer {
	Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
	
	private static boolean NO_DATA = false;
	private static int COUNTER = 0;
	private static int FAIL = 0;
	
	@Value("${server}")
	private String server;
	
	@Value("${consumerPort}")
	private String consumerPort;
	
	@Value("${producerPort}")
	private String producerPort;
	
	@Value("${topic}")
	private String topic;
	
	@Value("${opensearch}")
	private String opensearchIndex;
	
	@Value("${consumerGroup}")
	private String groupId;
	
	private RestHighLevelClient createOpenSearchClient() {
		RestHighLevelClient restClient;
		
		restClient = new RestHighLevelClient(RestClient.builder(
												new HttpHost(server, 
												Integer.parseInt(consumerPort), 
												"http")));
		
		return restClient;
	}
	
	private KafkaConsumer<String, String> createKafkaConsumer(){
		String bootstrapServer = server+":"+producerPort;
		
		Properties properties = new Properties();
		properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return new KafkaConsumer<String, String>(properties);
	}
	
	public void runConsumer() throws IOException {
		RestHighLevelClient opensearchClient = createOpenSearchClient();
		KafkaConsumer<String, String> consumer = createKafkaConsumer();
		
		try(opensearchClient; consumer){
			boolean indexExists = opensearchClient.indices()
												.exists(
														new GetIndexRequest(opensearchIndex), 
														RequestOptions.DEFAULT);
			
			if(!indexExists) {
				CreateIndexRequest indexRequest = new CreateIndexRequest(opensearchIndex);
				opensearchClient.indices().create(indexRequest, RequestOptions.DEFAULT);
			}else {
				logger.info("The Wikimedia index already exists");
			}
			
			consumer.subscribe(Collections.singleton(topic));
			while(COUNTER < 100) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
				int recordCount = records.count();
				logger.info("Received : "+recordCount+" record(s)");
				
				if(recordCount != 0) { NO_DATA = false; }
				
				BulkRequest bulkRequest = new BulkRequest();
				for(ConsumerRecord<String, String> record: records) {
					JSONObject json = new JSONObject(record.value());
					
					IndexRequest index = new IndexRequest(opensearchIndex)
												.source(json, XContentType.JSON);
					
					bulkRequest.add(index);
				}
				if(bulkRequest.numberOfActions() > 0) {
					BulkResponse response = opensearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
					logger.info("Inserted "+response.getItems().length+" document(s) in elasticsearch");
				}else {
					if(NO_DATA && FAIL > 5) { throw new WakeupException(); }
					else { NO_DATA = true; FAIL++;}
				}
				COUNTER++;
			}
		}catch(WakeupException ex) {
			logger.info("Consumer is shutting down.");
		}finally {
			consumer.close();
			opensearchClient.close();
			logger.info("Consumer is shut down.");
		}
	}
}
