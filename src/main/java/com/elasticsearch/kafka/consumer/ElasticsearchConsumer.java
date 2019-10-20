/*
 * package com.elasticsearch.kafka.consumer;
 * 
 * import java.io.IOException; import java.time.Duration; import
 * java.util.Arrays; import java.util.Properties;
 * 
 * import org.apache.http.HttpHost; import org.apache.http.auth.AuthScope;
 * import org.apache.http.auth.UsernamePasswordCredentials; import
 * org.apache.http.client.CredentialsProvider; import
 * org.apache.http.impl.client.BasicCredentialsProvider; import
 * org.apache.http.impl.nio.client.HttpAsyncClientBuilder; import
 * org.apache.kafka.clients.consumer.ConsumerConfig; import
 * org.apache.kafka.clients.consumer.ConsumerRecord; import
 * org.apache.kafka.clients.consumer.ConsumerRecords; import
 * org.apache.kafka.clients.consumer.KafkaConsumer; import
 * org.apache.kafka.common.serialization.StringDeserializer; import
 * org.apache.logging.log4j.Logger; import
 * org.elasticsearch.action.bulk.BulkRequest; import
 * org.elasticsearch.action.bulk.BulkResponse; import
 * org.elasticsearch.action.index.IndexRequest; import
 * org.elasticsearch.action.index.IndexResponse; import
 * org.elasticsearch.client.RequestOptions; import
 * org.elasticsearch.client.RestClient; import
 * org.elasticsearch.client.RestClientBuilder; import
 * org.elasticsearch.client.RestHighLevelClient; import
 * org.elasticsearch.common.xcontent.XContentType; import
 * org.slf4j.LoggerFactory; import
 * org.springframework.beans.factory.annotation.Autowired;
 * 
 * import com.elasticsearch.kafka.utilities.KafkaConsumerFactory; import
 * com.elasticsearch.kafka.utilities.RestClientFactory; import
 * com.elasticsearch.kafka.utilities.TweetJsonParsor; import
 * com.google.gson.JsonParser;
 * 
 * public class ElasticsearchConsumer {
 * 
 * @Autowired static RestClientFactory restClientFactory;
 * 
 * @Autowired static KafkaConsumerFactory kafkaConsumerFactory;
 * 
 * @Autowired static TweetJsonParsor tweetJsonParsor;
 * 
 * public static void main(String[] args) throws IOException {
 * 
 * org.slf4j.Logger logger =
 * LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());//
 * .getILoggerFactory();
 * 
 * RestHighLevelClient client = restClientFactory.getHighLevelClient();
 * 
 * KafkaConsumer<String, String> consumer =
 * kafkaConsumerFactory.getIdempotentConsumer();
 * 
 * String id = null;
 * 
 * while (true) {
 * 
 * ConsumerRecords<String, String> records =
 * consumer.poll(Duration.ofMillis(100));
 * 
 * Integer recordCount = records.count();
 * 
 * logger.info("Received " + records.count() + " records");
 * System.out.println("Received " + recordCount + " records");
 * 
 * BulkRequest bulkRequest = new BulkRequest();
 * 
 * for (ConsumerRecord<String, String> record : records) {
 * 
 * try {
 * 
 * id = tweetJsonParsor.extractIDFromTweet(record.value()); IndexRequest
 * indexRequest = new IndexRequest("twitter", "tweets",
 * id).source(record.value(), XContentType.JSON); // index,type // add a ID to
 * make our consumer idempotent bulkRequest.add(indexRequest); // adding to our
 * bulk request (takes no time)
 * 
 * } catch (NullPointerException e) { logger.warn("skipping bad data: " +
 * record.value()); }
 * 
 * }
 * 
 * if (recordCount > 0) { BulkResponse bulkItemResponses =
 * client.bulk(bulkRequest, RequestOptions.DEFAULT);
 * logger.info("Committing  offsets"); System.out.println("Committing offsets");
 * consumer.commitSync(); logger.info("Offsets committed");
 * System.out.println("Offsets committed");
 * System.out.println("*****************"); try { Thread.sleep(1000); } catch
 * (InterruptedException e) { // TODO Auto-generated catch block
 * e.printStackTrace(); } }
 * 
 * }
 * 
 * // client.close();
 * 
 * } }
 */