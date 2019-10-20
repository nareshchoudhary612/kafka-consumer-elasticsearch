package com.elasticsearch.kafka.service;

import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.elasticsearch.kafka.service.utilities.KafkaConsumerFactory;
import com.elasticsearch.kafka.service.utilities.RestClientFactory;
import com.elasticsearch.kafka.service.utilities.TweetJsonParsor;

@Service
public class KafkaToECService implements Runnable {
	

	public  void startSending() throws IOException {

		org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaToECService.class.getName());// .getILoggerFactory();

		RestHighLevelClient client = RestClientFactory.getHighLevelClient();

		KafkaConsumer<String, String> consumer = KafkaConsumerFactory.getIdempotentConsumer();

		String id = null;

		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			Integer recordCount = records.count();

			logger.info("Received " + records.count() + " records");
			System.out.println("Received " + recordCount + " records");

			BulkRequest bulkRequest = new BulkRequest();

			for (ConsumerRecord<String, String> record : records) {

				try {

					id = TweetJsonParsor.extractIDFromTweet(record.value());
					IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(),
							XContentType.JSON); // index,type // add a ID to make our consumer idempotent
					bulkRequest.add(indexRequest); // adding to our bulk request (takes no time)

				} catch (NullPointerException e) {
					logger.warn("skipping bad data: " + record.value());
				}

			}

			if (recordCount > 0) {
				BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Committing  offsets");
				System.out.println("Committing offsets");
				consumer.commitSync();
				logger.info("Offsets committed");
				System.out.println("Offsets committed");
				System.out.println("*****************");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		// client.close();

	}
	
	public void stopSending() throws IOException {
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		 
		
		
		org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaToECService.class.getName());// .getILoggerFactory();
		
		
		
		RestHighLevelClient client = RestClientFactory.getHighLevelClient();

		KafkaConsumer<String, String> consumer = KafkaConsumerFactory.getIdempotentConsumer();

		String id = null;

		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			Integer recordCount = records.count();

			logger.info("Received " + records.count() + " records");
			System.out.println("Received " + recordCount + " records");

			BulkRequest bulkRequest = new BulkRequest();

			for (ConsumerRecord<String, String> record : records) {

				try {

					id = TweetJsonParsor.extractIDFromTweet(record.value());
					IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(),
							XContentType.JSON); // index,type // add a ID to make our consumer idempotent
					bulkRequest.add(indexRequest); // adding to our bulk request (takes no time)

				} catch (NullPointerException e) {
					logger.warn("skipping bad data: " + record.value());
				}

			}
			
			/* if records are not present then skip sending to ES */
			if (recordCount > 0) {
				try {
					BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				logger.info("Committing  offsets");
				System.out.println("Committing offsets");
				consumer.commitSync();
				logger.info("Offsets committed");
				System.out.println("Offsets committed");
				System.out.println("*****************");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		
	}
}
