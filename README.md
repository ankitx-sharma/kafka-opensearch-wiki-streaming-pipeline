# Real-Time Wikipedia Streaming Pipeline with Kafka & OpenSearch
This project demonstrates a complete real-time data pipeline using Spring Boot, Apache Kafka, and OpenSearch.  
It streams live Wikipedia edits from Wikimedia EventStreams, publishes them into Kafka, and then consumes and indexes the data into OpenSearch for fast searching and analysis.

### Features
- Streams live Wikipedia RecentChange events (SSE stream)
- Kafka producer sends raw JSON data to a topic
- Kafka consumer reads messages and bulk-indexes them into OpenSearch
- Automatic startup & shutdown of Kafka/OpenSearch via Docker
- Uses Spring Boot for orchestration
- Handles JSON errors, bulk operations, and controlled shutdowns

### Architecture Overview
```
Wikimedia EventStream
        |
        v
Kafka Producer  --->  Kafka Topic (wikimedia_recentchange)
        |
        v
Kafka Consumer  --->  OpenSearch Index (wikimedia)
```
This pipeline converts live Wikipedia edits into a searchable dataset.

### Technologies Used
- Java 17+ / Spring Boot
- Apache Kafka
- OpenSearch (Elasticsearch compatible)
- Docker & Docker Compose
- JSON Processing

### How It Works
1. Application Startup
When Spring Boot launches:
- Docker start commands are executed (docker-start.txt)
- Kafka and OpenSearch containers are started automatically

2. Producer Stage
The WikimediaProducer:
- Connects to
```
https://stream.wikimedia.org/v2/stream/recentchange
```
- Receives live JSON events
- Publishes each event to the Kafka topic:
```
wikimedia_recentchange
```
3. Consumer Stage
The OpenSearchConsumer:
- Subscribes to the Kafka topic
- Reads messages in batches
- Creates the OpenSearch index if needed
- Inserts documents using the Bulk API

4. Shutdown
After processing:
- Docker stop commands are executed (docker-end.txt)
- Kafka & OpenSearch gracefully shut down

### Running the Project
**Prerequisites**
- Docker installed
- Java 17+
- Maven

**Command**
```
mvn spring-boot:run
```
