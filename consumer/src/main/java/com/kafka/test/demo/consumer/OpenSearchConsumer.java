package com.kafka.test.demo.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

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
	
	@Value("${kafka.bootstrap}")
	private String kafkaBootstrapServer;
	
	@Value("${kafka.topic}")
	private String kafkaTopic;
	
	@Value("${kafka.groupId}")
	private String groupId;
	
	@Value("${kafka.autoOffsetReset:earliest}")
	private String autoOffsetReset;
	
	@Value("${opensearch.host}")
	private String openSearchHost;
	
	@Value("${opensearch.port:9200}")
    private int openSearchPort;
	
	@Value("${opensearch.scheme:http}")
    private String openSearchScheme;
	
	@Value("${opensearch.index:wikimedia}")
    private String indexName;
	
	@Value("${consumer.pollSeconds:3}")
    private int pollSeconds;
	
	@Value("${consumer.idleStopSeconds:30}")
    private int idleStopSeconds;
	
	public void runConsumer() throws IOException {
		logger.info("Staring consumer with bootstrap={}, topic={}, groupId={}", kafkaBootstrapServer, kafkaTopic, groupId);
		logger.info("OpenSearch target: {}://{}:{}, index={}", openSearchScheme, openSearchHost, openSearchPort, indexName);
		logger.info("Stop rule: exit after {}s of no data (poll every {}s)", idleStopSeconds, pollSeconds);
		
		try(RestHighLevelClient opensearchClient = createOpenSearchClient()){
			ensureIndexExists(opensearchClient);
			
			try(KafkaConsumer<String, String> consumer = createKafkaConsumer()){
				consumer.subscribe(Collections.singletonList(kafkaTopic));
				
				final AtomicBoolean running = new AtomicBoolean(true);
				
				//Clean shutdown
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
					running.set(false);
					try {
						consumer.wakeup();
					}catch(Exception ex) {
						//ignored
					}
				}));
				
				final long idleStopMs = idleStopSeconds * 1000L;
				long lastMessageAt = System.currentTimeMillis();
				
				try {
					while(running.get()) {
						ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollSeconds));
						int recordCount = records.count();
						logger.info("Received : "+recordCount+" record(s)");
						
						if(!records.isEmpty()) {
							lastMessageAt = System.currentTimeMillis();
							
							BulkRequest bulkRequest = new BulkRequest();
							for(ConsumerRecord<String, String> record: records) {
								try {
									JSONObject json = new JSONObject(record.value());
									
									IndexRequest index = new IndexRequest(indexName)
																.source(json, XContentType.JSON);
									
									bulkRequest.add(index);
								} catch (Exception ex) {
									logger.warn("Skipping record due to bad JSON/index payload: {}", ex.getMessage());
								}
							}
							
							if(bulkRequest.numberOfActions() > 0) {
								BulkResponse response = opensearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
								
								if(response.hasFailures()) {
									logger.error("Bulk indexing had failures: {}", response.buildFailureMessage());
								}else {
									consumer.commitSync();
									logger.info("Inserted "+response.getItems().length+" document(s) in elasticsearch");
								}
							} else {
								logger.warn("All polled records were skipped. No commit performed.");
							}
						} else {
							long idleFor = System.currentTimeMillis() - lastMessageAt;
                            if (idleFor >= idleStopMs) {
                                logger.info("No messages for {} seconds. Shutting down consumer.", idleStopSeconds);
                                break;
                            }
						}
					}
				} catch (WakeupException ex) {
					logger.info("Consumer wakeup triggered. Exiting...");
				} catch (Exception e) {
                    logger.error("Consumer crashed: ", e);
                    throw e;
                } finally {
                    logger.info("Consumer shutting down...");
                }
			}
		}
	}
	
	private RestHighLevelClient createOpenSearchClient() {
		return new RestHighLevelClient(RestClient.builder(
					new HttpHost(openSearchHost, openSearchPort, openSearchScheme)));
	}
	
	private KafkaConsumer<String, String> createKafkaConsumer(){
		
		Properties properties = new Properties();
		properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		
		// Disable auto-commit; we commit only after successful OpenSearch indexing
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		return new KafkaConsumer<String, String>(properties);
	}
	
	private void ensureIndexExists(RestHighLevelClient opensearchClient) throws IOException{
		boolean indexExists = opensearchClient.indices()
				.exists(
						new GetIndexRequest(indexName), 
						RequestOptions.DEFAULT);
		
		if(!indexExists) {
			logger.info("Index '{}' does not exist. Creating ...", indexName);
			opensearchClient.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
			logger.info("Index '{}' created.", indexName);
		}else {
			logger.info("The index '{}' already exists", indexName);
		}

	}
}
