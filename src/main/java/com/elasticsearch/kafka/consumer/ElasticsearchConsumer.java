package com.elasticsearch.kafka.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticsearchConsumer {

	public static RestHighLevelClient creatClient() {

		// https://xgml3ka9qv:ys5srjvgbo@kafka-twitter-7404311451.us-west-2.bonsaisearch.net:443

		String hostname = "kafka-twitter-7404311451.us-west-2.bonsaisearch.net";
		String username = "xgml3ka9qv";
		String password = "ys5srjvgbo";

		// not required in case of local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}

				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

	public static KafkaConsumer<String, String> createConsumer() {

		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "Group1";
		String topic = "second_topic";

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // manually committing offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}
	
	private static JsonParser jsonParser = new JsonParser();
	private static String extractIDFromTweet(String tweet) {
		return jsonParser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
		
	}

	public static void main(String[] args) throws IOException {

		org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());// .getILoggerFactory();

		RestHighLevelClient client = creatClient();

		
		

		
		
		KafkaConsumer<String, String> consumer = createConsumer();
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			logger.info("Received " + records.count() + " records");
			System.out.println("Received " + records.count() + " records");
			for(ConsumerRecord<String,String> record:records) {
				
				
				//2 ways to create ID
				
				//First way
				//String id = record.topic() + "_" + record.partition() + "_" + record.offset();
				
				//Second way
				String id =  extractIDFromTweet(record.value());
				// insert data into ES
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets",id).source(record.value(), XContentType.JSON); // index,type
				// add a ID to make our consumer idempotent
				
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

				logger.info(indexResponse.getId());
				System.out.println(indexResponse.getId());
				//System.out.println(indexRequest.get);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
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
		

		//client.close();

	}
}
